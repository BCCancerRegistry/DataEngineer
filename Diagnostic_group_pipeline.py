import os.path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models.param import Param
import pandas as pd
import os
import sys
import model
#import BCCancer
from datetime import date
from BCCancer import get_data
from BCCancer import save_data
from BCCancer.model.tor_model import Torch_tokenizer
from BCCancer.model.dggroup import apply_model
from BCCancer.data_processing.prep_msg import clean_msg, create_sections, create_section_regex
import data_prep
# Define default_args for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 15),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Data cleaning function
def clean_data(**kwargs):
    ti = kwargs["ti"]
    """"input_db_name": Param(type="string"),
        "input_db_type": Param(type="string", enum=['sql-server', 'postgres']),
        "input_db_username":Param(type="string"),
        "input_db_password": Param(type = "string"),
        "input_db_port":Param(type = ["int","null"], default = 'null'),
        "source_data_table": Param (type='string'),
        "source_table_cols": Param(type='string'),
        "source_date_col": Param(type = 'string'),
        "source_date_to":Param(type = 'string'),
        "source_date_from":Param(type = 'string'),
        "labels_table":Param(type = 'string', default="dg_labels"),
        "output_db_name": Param(type="string"),
        "output_db_type": Param(type="string", enum=['sql-server', 'postgres']),
        "output_db_username": Param(type="string"),
        "output_db_password": Param(type="string"),
        "output_db_port": Param(type = ["int","null"], default='null'),
        "clean_data_table": Param(default='cleaned_data',type="string"),
        "prepped_data_table": Param(default='prepped_data',type="string"),
        "prediction_table": Param(default='prediction',type="string"),
"""
    model_path = kwargs["params"]["model_path"]
    model_name = kwargs["params"]["model_name"]
    input_db_type = kwargs["params"]["input_db_type"]
    input_db_name = kwargs["params"]["input_db_name"]
    input_db_server = kwargs["params"]["input_db_server"]
    input_db_port = kwargs["params"]["input_db_port"]
    input_db_username = kwargs["params"]["input_db_username"]
    input_db_password = kwargs["params"]["input_db_password"]
    input_table = kwargs["params"]["source_data_table"]
    input_table_columns = kwargs["params"]["source_table_cols"]
    input_date_column = kwargs["params"]["source_date_col"]
    input_date_to = kwargs["params"]["source_date_to"]
    input_date_from = kwargs["params"]["source_date_from"]
    label_table = kwargs["params"]["labels_table"]
    #cleanned_data_table = kwargs["params"]["data_path"]
    prediction_table = kwargs["params"]["data_path"]
    cleaned_data_table = kwargs["params"]["data_path"]
    output_db_type = kwargs["params"]["data_path"]
    output_db_name = kwargs["params"]["data_path"]
    output_db_server = kwargs["params"]["data_path"]
    output_db_port = kwargs["params"]["data_path"]
    output_db_username = kwargs["params"]["data_path"]
    output_db_password = kwargs["params"]["data_path"]
    data=get_data.get_messages(db_type=input_db_type,server=input_db_server,database=input_db_name,
                               username=input_db_username,password=input_db_password,port=input_db_port,source_table=input_table,
                               date_column=input_date_column,date_from=input_date_from,date_to=input_date_to,col_list=['msgid','message'])

    pipeline_name="DX_Group"
    date_to = input_date_to
    date_from=input_date_from
    comment = f"Pipeline ran with parameters : {kwargs['params'].__str__()}"
    batch_row=pd.DataFrame.from_records([dict(pipeline=pipeline_name, date_to=date_to, date_from=date_from, comment=comment)])
    save_batch_info = dict(
        db_type=output_db_type, server=output_db_server, database=output_db_name,
        username=output_db_username, password=output_db_password, port=output_db_port,
        dest_table="batch", data=batch_row
    )
    create_batch = save_data(save_batch_info)
    get_batch_info =
    data['msg'] = data['MESSAGE'].apply(clean_msg)
    data['batch_id']=batch_id
    data= data[['batch_id','msgid','msg']]
    save_output = dict(
        db_type=output_db_type, server=output_db_server, database=output_db_name,
        username=output_db_username, password=output_db_password, port=output_db_port,
        dest_table="cleanned_msg",data=data
    )
    #dest_table:str,data:pd.DataFrame,
    save_data(**save_output)
    df.to_csv(path)
    ti.xcom_push(key="cleaned_data", value={"path": path})


def prep_data(**kwargs):
    pass

# Prediction function
def predict_data(**kwargs):
    ti = kwargs["ti"]
    output_path = kwargs["params"]["output_path"]
    model_path = kwargs["params"]["model_path"]
    model_name = kwargs["params"]["model_name"]
    cleaned_data = ti.xcom_pull(task_ids="clean_data_task", key="cleaned_data")
    path = cleaned_data["path"]
    data = data_prep.load_data(path)
    pred_data = model.apply_model(os.path.join(model_path,model_name),data, LoggingMixin())
    pred_data.to_csv(output_path)






# Get data function

# DAG definition
with DAG(
    "MVP_DAG",
    default_args=default_args,
    schedule_interval=None,
    params = {
        "model_path": Param(default="/tmp/model", type="string"),
        "model_name": Param(default="pathologyBERT_canvsnoncan", type="string"),
        "input_db_server": Param(type="string"),
        "input_db_name": Param(type="string"),
        "input_db_type": Param(type="string", enum=['sql-server', 'postgres']),
        "input_db_username":Param(type="string"),
        "input_db_password": Param(type = "string"),
        "input_db_port":Param(type = ["int","null"], default = 'null'),
        "source_data_table": Param (type='string'),
        "source_table_cols": Param(type='string'),
        "source_date_col": Param(type = 'string'),
        "source_date_to": Param(type = 'string'),
        "source_date_from": Param(type = 'string'),
        "labels_table": Param(type = 'string', default="dg_labels"),
        "output_db_server": Param(type="string"),
        "output_db_name": Param(type="string"),
        "output_db_type": Param(type="string", enum=['sql-server', 'postgres']),
        "output_db_username": Param(type="string"),
        "output_db_password": Param(type="string"),
        "output_db_port": Param(type = ["int","null"], default='null'),
        "clean_data_table": Param(default='cleaned_data',type="string"),
        "prepped_data_table": Param(default='prepped_data',type="string"),
        "prediction_table": Param(default='prediction',type="string"),


}
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
        task_id="data_prep",
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

    #train_or_skip_instance = use_trained_model("{{param.skip_training}}","{{param.model_path}}")
    data_cleaning_task >> data_prep >> predict_task
