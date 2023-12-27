import os.path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models.param import Param
import pandas as pd
import os
import sys

# import model
# import BCCancer
from datetime import date
from BCCancer.get_data import get_messages
from BCCancer.get_data import save_data
from BCCancer.conn_manager.postgres_conn import PostgresConn
from BCCancer.conn_manager.sql_conn import SqlserverConn
from BCCancer.model.tor_model import Torch_tokenizer, Torch_model
from BCCancer.model.dggroup import apply_model
from BCCancer.data_processing.prep_msg import (
    clean_msg,
    create_sections,
    create_section_regex,
)

# import data_prep
# Define default_args for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 15),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Data cleaning function
def create_sql(table, columns, conditions: [None, list] = None):
    col_str = ", ".join(columns)
    condition_str = None if conditions is None else " and ".join(conditions)
    sql = f"select {col_str} from {table} " + (
        "" if conditions is None else f"where {condition_str}"
    )
    print(sql)
    return sql


def truncate_text(text, part_of_report, tokenizer):
    encoding = tokenizer(text, return_offsets_mapping=True)[0]
    a=0
    if len(encoding) > 510:
        if part_of_report == "entire report":  # Use the LAST 510 tokens in the report
            start = encoding.offsets[len(encoding) - 510][1]
            a=start
            print(f"start:{start}")
            text = text[start:]
        elif part_of_report in [
            "gross_or_micro",
            "diag_or_add",
        ]:  # Use the first 510 tokens
            end = encoding.offsets[510][1]
            a=end
            print(f"end:{end}")
            text = text[:end]
    if len(text) > 510:
        print("Text Length: " + str(len(text)))
        print("part of speech")
    return (text,a)


def format_date(date: str, date_format: str):
    return datetime.strptime(date, date_format).strftime("%Y%m%d")


def clean_data(**kwargs):
    ti = kwargs["ti"]
    api_url = kwargs["params"]["api_url"]
    api_token = kwargs["params"]["api_token"]
    # cleanned_data_table = kwargs["params"]["data_path"]
    output_db_type = kwargs["params"]["output_db_type"]
    output_db_name = kwargs["params"]["output_db_name"]
    output_db_server = kwargs["params"]["output_db_server"]
    output_db_port = kwargs["params"]["output_db_port"]
    output_db_username = kwargs["params"]["output_db_username"]
    output_db_password = kwargs["params"]["output_db_password"]
    date_from = kwargs["params"]["from_date"]
    date_to = kwargs["params"]["to_date"]
    """data = get_messages(
        db_type=input_db_type,
        server=input_db_server,
        database=input_db_name,
        username=input_db_username,
        password=input_db_password,
        port=input_db_port,
        source_table=input_table,
        date_column=input_date_column,
        date_from=input_date_from,
        date_to=input_date_to,
        col_list=["msgid", "message"],
    )"""
    data = get_messages(api_url, date_from, date_to, api_token)
    data = pd.DataFrame.from_records(data)
    pipeline_name = "DX_Group_with_api"
    comment = f"Pipeline ran with parameters : {kwargs['params'].__str__()}"
    batch_row = dict(
        pipeline_name=pipeline_name,
        date_to=date_to,
        date_from=date_from,
        comment=comment,
    )
    logger = LoggingMixin()
    if output_db_type == "postgres":
        with PostgresConn(
            host=output_db_server,
            database_name=output_db_name,
            username=output_db_username,
            password=output_db_password,
            port=output_db_port,
        ) as conn:
            saved_row = conn.insert_row(table_name="batch", column_values=batch_row)
            logger.log.info(type(saved_row))
            logger.log.info(saved_row)
    else:
        raise NotImplementedError

    logger.log.info(type(saved_row))
    logger.log.info(saved_row)
    batch_id = saved_row["batch_id"]

    data["msg"] = data["message"].apply(clean_msg)
    data["batch_id"] = batch_id
    data = data[["batch_id", "msgid", "msg"]]
    if output_db_type == "postgres":
        with PostgresConn(
            host=output_db_server,
            database_name=output_db_name,
            username=output_db_username,
            password=output_db_password,
            port=output_db_port,
        ) as conn:
            conn.insert_data(data, table="cleaned_data")
            logger.log.info(type(saved_row))
            logger.log.info(saved_row)
    else:
        raise NotImplementedError
    # dest_table:str,data:pd.DataFrame,
    ti.xcom_push(key="batch_id", value={"batch_id": batch_id})


def prep_data(**kwargs):
    print("in prep data")
    ti = kwargs["ti"]
    model_name = kwargs["params"]["model_name"]
    model_version = kwargs["params"]["model_version"]
    output_db_type = kwargs["params"]["output_db_type"]
    output_db_name = kwargs["params"]["output_db_name"]
    output_db_server = kwargs["params"]["output_db_server"]
    output_db_port = kwargs["params"]["output_db_port"]
    output_db_username = kwargs["params"]["output_db_username"]
    output_db_password = kwargs["params"]["output_db_password"]
    batch_id = ti.xcom_pull(task_ids="clean_data_task", key="batch_id")

    print(batch_id)
    columns = ["batch_id", "msgid", "msg"]
    conditions = [f"batch_id={batch_id['batch_id']}"]
    table = "cleaned_data"
    sql = create_sql(table=table, columns=columns, conditions=conditions)
    print(sql)
    model_columns = ["model_id"]
    get_model_sql = create_sql(
        table="model",
        columns=model_columns,
        conditions=[f"model_name='{model_name}'", f"model_version='{model_version}'"],
    )
    output_table = "preped_data"
    output_columns = [
        "batch_id",
        "msgid",
        "gross",
        "addendum",
        "diagnosis",
        "diagnosis_comment",
        "micro",
        "filtered_message",
        "part_of_report",
    ]
    if output_db_type == "postgres":
        with PostgresConn(
            host=output_db_server,
            database_name=output_db_name,
            username=output_db_username,
            password=output_db_password,
            port=output_db_port,
            echo=True,
        ) as conn:
            model_info = conn.get_data(get_model_sql, model_columns)
            model_id = model_info.iloc[0][0]
            section_regex_condition = [f"model_id={model_id}"]
            section_regex_columns = [
                "parent_category",
                "nha",
                "fha",
                "fha2",
                "iha",
                "vcha1",
                "vcha2",
            ]
            section_regex_table = "section_regex"
            section_df = conn.get_data(
                create_sql(
                    table=section_regex_table,
                    columns=section_regex_columns,
                    conditions=section_regex_condition,
                ),
                columns=section_regex_columns,
            )
            section_regex = create_section_regex(section_df)
            data = conn.get_data(sql, columns)
            section = data.apply(
                lambda x: create_sections(x["msg"], section_regex=section_regex), axis=1
            )
            section = pd.DataFrame.from_dict(section.to_list())
            data[section.columns] = section
            print(f"Inserting data in {output_table}:")
            print(data[output_columns])
            conn.insert_data(data[output_columns], table=output_table)
    else:
        raise NotImplementedError
    ti.xcom_push(key="output_table", value=dict(output_table=output_table))


# Prediction function
def predict_data(**kwargs):
    ti = kwargs["ti"]
    batch_id = ti.xcom_pull(task_ids="clean_data_task", key="batch_id")

    model_name = kwargs["params"]["model_name"]
    model_version = kwargs["params"]["model_version"]
    model_tokenizer = kwargs["params"]["model_tokenizer"]
    prepped_data = ti.xcom_pull(task_ids="data_prep_task", key="output_table")[
        "output_table"
    ]
    api_url = kwargs["params"]["api_url"]
    api_token = kwargs["params"]["api_token"]
    output_db_type = kwargs["params"]["output_db_type"]
    output_db_name = kwargs["params"]["output_db_name"]
    output_db_server = kwargs["params"]["output_db_server"]
    output_db_port = kwargs["params"]["output_db_port"]
    output_db_username = kwargs["params"]["output_db_username"]
    output_db_password = kwargs["params"]["output_db_password"]
    conditions = [f"batch_id={batch_id['batch_id']}"]
    columns = ["batch_id", "msgid", "filtered_message", "part_of_report"]
    sql = create_sql(table=prepped_data, columns=columns, conditions=conditions)
    model_columns = ["model_id", "model_location"]
    get_model_sql = create_sql(
        table="model",
        columns=model_columns,
        conditions=[f"model_name='{model_name}'", f"model_version='{model_version}'"],
    )

    # final_msg_column = "truncated_message"
    output_columns = ["batch_id", "msgid", "predicted_label", "model_score", "model_id"]
    print(f"Tokenizer path: {model_tokenizer}")
    if output_db_type == "postgres":
        with PostgresConn(
            host=output_db_server,
            database_name=output_db_name,
            username=output_db_username,
            password=output_db_password,
            port=output_db_port,
        ) as conn:
            model_info = conn.get_data(get_model_sql, model_columns)
            model_id, model_location = model_info.iloc[0]
            model = Torch_model(
                model_name=model_name,
                model_location=model_location,
                tokenizer_path=model_tokenizer,
                num_labels=16,
                max_length=512,
                use_fast=True,
                device=-1,
            )

            data = conn.get_data(sql, columns=columns)
            tok = model.tok
            final_column = pd.DataFrame.from_records(data.apply(
                lambda x: truncate_text(
                    x["filtered_message"], x["part_of_report"], tok
                ),
                axis=1,
            ))
            final_column.columns = ["filtered_message","offset"]
            # print("final_columns:"+final_column.columns)
            # print(f"final_column shape:{final_column.shape}")
            log_df = pd.DataFrame(
                {"msgid": data["msgid"], "filtered_message": final_column["filtered_message"], "offset":final_column['offset']}
            )
            #log_df.to_csv("/data/logtruncatedtext.csv")
            data[["predicted_label", "model_score"]] = model.apply_model(final_column["filtered_message"])
            data["predicted_label"] = data["predicted_label"].apply(
                lambda x: x.split("_")[1]
            )
            data["model_id"] = model_id
            conn.insert_data(data[output_columns], table="prediction_table")
            labels_sql = f"select model_id, label, label_name from labels where model_id = {model_id}"
            labels_columns = ['model_id','label','label_name']
            labels_df = conn.get_data(labels_sql, columns=labels_columns)
            data.predicted_label = data.predicted_label.astype("int64")
            output_data = pd.merge(data, labels_df, left_on='predicted_label', right_on='label')
            output_data['model_id']=model_id
            output_data = output_data[["batch_id", "msgid", "predicted_label", "model_score", "model_id","label_name"]]
            output_data.columns = ["batchid", "msgid", "predicted_label_id", "model_score", "model_id","predicted_label"]
            output_path = f"/tmp/dx_output{batch_id}.csv"
            output_data.to_csv(output_path, index=False)
            save_data(api_url,output_path,api_token)
    else:
        raise NotImplementedError


# Get data function

# DAG definition
with DAG(
    "DX_Group_classification_with_api",
    default_args=default_args,
    schedule_interval=None,
    params={
        "api_url": Param(default="http://10.42.121.98:6000", type="string"),
        "api_token": Param(default="null", type="string"),
        "from_date": Param(default = "20220101", type = "string"),
        "to_date": Param(default = "20230101", type = "string"),
        "model_path": Param(default="null", type=["string", "null"]),
        "model_name": Param(
            default="clinicalBERT_16class_onco_Dxgroup_10k", type="string"
        ),
        "model_version": Param(default=1, type="integer"),
        "model_tokenizer": Param(
            default="/data/models/Bio_ClinicalBERT", type="string"
        ),
        "output_db_server": Param(type="string", default="postgres"),
        "output_db_name": Param(type="string", default="BCCancer_DE"),
        "output_db_type": Param(
            type="string", enum=["sql-server", "postgres"], default="postgres"
        ),
        "output_db_username": Param(type="string", default="bccancer_de_rw_user"),
        "output_db_password": Param(type="string", default="bccancerdataengineering"),
        "output_db_port": Param(type=["integer", "null"], default=5432),
    },
) as dag:
    """db_type: str, server: str, database: str, username: str, password: str,port:int, source_table:str,col_list:list,date_column:str=None, date_to:str=None, date_from:str=None"""
    # Get data task

    # Data cleaning task
    data_cleaning_task = PythonOperator(
        task_id="clean_data_task",
        python_callable=clean_data,
        provide_context=True,
        dag=dag,
    )

    # Model training task
    """
    train_task = PythonOperator(
        task_id="train_model_task",
        python_callable=train_model,
        provide_context=True,
        dag=dag,
    )"""

    data_prep = PythonOperator(
        task_id="data_prep_task",
        python_callable=prep_data,
        provide_context=True,
        dag=dag,
    )

    # Prediction task
    predict_task = PythonOperator(
        task_id="predict_model_task",
        python_callable=predict_data,
        provide_context=True,
        dag=dag,
    )

    # Conditional task based on user choice

    """@task.branch(task_id="train_or_skip")
    def use_trained_model(skip_training, model_path:None):
        skip_training = kwargs["dag_run"].conf.get("skip_training", False)
        model_path = kwargs["dag_run"].conf.get("model_path", False)
        if model_path is None:
            return "train"
        if skip_training:
            if os.path.exists(model_path):
                return "skip"
        return "train"


    skip_training_task = PythonOperator(
        task_id="skip_training_task",
        python_callable=skip_training,
        provide_context=True,
        dag=dag,
    )
    """
    # Task dependencies

    # train_or_skip_instance = use_trained_model("{{param.skip_training}}","{{param.model_path}}")
    data_cleaning_task >> data_prep >> predict_task
