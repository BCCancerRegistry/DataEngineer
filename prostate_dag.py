import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime, timedelta
from BCCancer.model.longformer_model import LongformerModel
from airflow.utils.log.logging_mixin import LoggingMixin
from BCCancer.conn_manager.postgres_conn import PostgresConn

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def load_and_preprocess():
    """Currently Not required as done in the DG group pipeline"""
    pass


def get_questions(model_id, **kwargs):
    sql = f"select sections,questions from model_questions where model_id={model_id}"
    questions = get_data(sql, **kwargs)
    return questions.to_dict()


def load_data_from(table, batch_id, model_columns, **kwargs):
    sql = f"select * from {table} where batch_id={batch_id}"
    params = get_dag_params(**kwargs)
    with PostgresConn(
        host=params["output_db_server"],
        database_name=params["output_db_name"],
        username=params["output_db_username"],
        password=params["output_db_password"],
        port=params["output_db_port"],
    ) as conn:
        data = conn.get_data(sql, model_columns)

    return data


def format_date(date: str, date_format: str):
    return datetime.strptime(date, date_format).strftime("%Y%m%d")


def create_batch(**kwargs):
    ti = kwargs["ti"]
    output_db_type = kwargs["params"]["output_db_type"]
    output_db_name = kwargs["params"]["output_db_name"]
    output_db_server = kwargs["params"]["output_db_server"]
    output_db_port = kwargs["params"]["output_db_port"]
    output_db_username = kwargs["params"]["output_db_username"]
    output_db_password = kwargs["params"]["output_db_password"]
    pipeline_name = "Prostate_pipeline"
    comment = f"Pipeline ran with parameters : {kwargs['params'].__str__()}"
    params = get_dag_params(**kwargs)
    data = load_data_from(
        "batch", batch_id=params["batch_id"], model_columns=[], **kwargs
    )
    date_from = data.date_from[0]
    date_to = data.date_to[0]
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
    ti.xcom_push(key="batch_id", value={"batch_id": batch_id})


def create_sections(**kwargs):
    params = get_dag_params(**kwargs)
    model_name = params["model_name"]
    model_version = 1
    model_id = get_data(
        f"select model_id from model where model_name={model_name} and model_version={model_version}"
    )
    questions = get_questions(model_id, **kwargs)
    params = get_dag_params(**kwargs)
    data = load_data_from(
        "cleaned_data", batch_id=params["batch_id"], model_columns=[], **kwargs
    )
    model = LongformerModel(
        model_location=params["longformer_location"],
        tokenizer_location=params["Tokenizer_location"],
    )

    required_sections = [
        "Comment",
        "Addendum",
        "Gross description",
        "Diagnosis",
        "Clinical History",
        "Microscopic",
        "Overall report",
    ]

    sections_df = pd.DataFrame.from_records(
        data["text"].apply(
            lambda text: {
                i: model.answerquestion(questions[i], text) for i in required_sections
            }
        )
    )
    sections_table = "preped_data"
    save_data(sections_df, sections_table, **kwargs)


def save_data(data, table, **kwargs):
    pass


def get_data(sql, columns, **kwargs):
    params = get_dag_params(**kwargs)
    with PostgresConn(
        host=params["output_db_server"],
        database_name=params["output_db_name"],
        username=params["output_db_username"],
        password=params["output_db_password"],
        port=params["output_db_port"],
    ) as conn:
        data = conn.get_data(sql, columns)

    return data


def invasive_surgery_prediction(**kwargs):
    pass


def neoadjuvant_prediction(**kwargs):
    pass


def lvi_prediction(**kwargs):
    pass


def histology_model(**kwargs):
    pass


def addendum(**kwargs):
    pass


def sitecodeandbehaviour(**kwargs):
    params = get_dag_params(**kwargs)
    data = load_data_from(
        "cleaned_data", batch_id=params["batch_id"], model_columns=[], **kwargs
    )
    data["site_code"] = "C61.9"
    data["site_code_pred_score"] = 1.0
    data["behaviour_code"] = "3"
    data["behaviour_code_pred_score"] = 1.0
    data["laterality"] = "00"
    data["laterality_pred_score"] = 1.0


output_columns = [
    "msg_id",
    "site_code",
    "site_code_pred_score",
    "histology_code",
    "histology_code_pred_score",
    "behaviour_code",
    "behaviour_code_pred_score",
    "laterality",
    "laterality_pred_score",
    "lymph_vascular_invasion",
    "lymph_vascular_invasion_pred_score",
    "invasive_surgery_flag",
    "invasive_surgery_flag_pred_score",
    "addendum_section_flag",
    "addendum_section_flag_pred_score",
    "diagnostic_procedure_flag",
    "diagnostic_procedure_pred_score",
    "neoadjuvant_therapy_flag",
    "neoadjuvant_therapy_pred_score",
]


def finalize_results(**kwargs):
    pass


def get_dag_params(**kwargs):
    params = kwargs["params"]
    return params


with DAG(
    "Prostate_pileline",
    default_args=default_args,
    schedule_interval=None,
    params={
        "hist_model_path": Param(default="null", type=["string", "null"]),
        "hist_model_name": Param(default="histology_model", type="string"),
        "hist_model_version": Param(default=-1, type="int"),
        "hist_model_tokenizer": Param(
            default="emilyalsentzer/Bio_ClinicalBERT", type="string"
        ),
        "lvi_model_path": Param(default="null", type=["string", "null"]),
        "lvi_model_name": Param(default="pathologyBERT_canvsnoncan", type="string"),
        "lvi_model_version": Param(default=-1, type="int"),
        "lvi_model_tokenizer": Param(default="/tmp/model/tor_tokenizer", type="string"),
        "inv_model_path": Param(default="null", type=["string", "null"]),
        "inv_model_name": Param(default="invasive_surgery_model", type="string"),
        "inv_model_version": Param(default=-1, type="int"),
        "inv_model_tokenizer": Param(
            default="emilyalsentzer/Bio_ClinicalBERTr", type="string"
        ),
        "labels_table": Param(type="string", default="dg_labels"),
        "output_db_server": Param(type="string"),
        "output_db_name": Param(type="string"),
        "output_db_type": Param(type="string", enum=["sql-server", "postgres"]),
        "output_db_username": Param(type="string"),
        "output_db_password": Param(type="string"),
        "output_db_port": Param(type=["int", "null"], default="null"),
        "clean_data_table": Param(default="cleaned_data", type="string"),
        "prepped_data_table": Param(default="prepped_data", type="string"),
        "prediction_table": Param(default="prediction", type="string"),
    },
) as dag:
    """db_type: str, server: str, database: str, username: str, password: str,port:int, source_table:str,col_list:list,date_column:str=None, date_to:str=None, date_from:str=None"""
    # Get data task

    # Data cleaning task
    create_b = PythonOperator(
        task_id="create_batch",
        python_callable=create_batch,
        provide_context=True,
        dag=dag,
    )
    create_segments = PythonOperator(
        task_id="create_segments",
        python_callable=create_sections,
        provide_context=True,
        dag=dag,
    )

    # Prediction task
    invasive_surgery = PythonOperator(
        task_id="invasive_surgery",
        python_callable=invasive_surgery_prediction,
        provide_context=True,
        dag=dag,
    )

    neoadjuvant_therapy = PythonOperator(
        task_id="invasive_surgery",
        python_callable=neoadjuvant_prediction,
        provide_context=True,
        dag=dag,
    )

    lvi_pred = PythonOperator(
        task_id="lymphovascular_invasion_pred",
        python_callable=lvi_prediction,
        provide_context=True,
        dag=dag,
    )

    hist_pred = PythonOperator(
        task_id="histology_pred",
        python_callable=histology_model,
        provide_context=True,
        dag=dag,
    )

    addendum_prep = PythonOperator(
        task_id="addendum_pred",
        python_callable=addendum,
        provide_context=True,
        dag=dag,
    )

    site_code = PythonOperator(
        task_id="sitecode_behaviour_laterality",
        python_callable=sitecodeandbehaviour,
        provide_context=True,
        dag=dag,
    )

    merge_data = PythonOperator(
        task_id="finalize_results",
        python_callable=finalize_results,
        provide_context=True,
        dag=dag,
    )

    # Task dependencies

    create_b >> create_segments >> lvi_pred >> merge_data
    create_b >> create_segments >> hist_pred >> merge_data
    create_b >> neoadjuvant_therapy >> merge_data
    create_b >> invasive_surgery >> merge_data
    create_b >> addendum_prep >> merge_data
    create_b >> site_code >> merge_data
