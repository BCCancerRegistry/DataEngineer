import os.path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models.param import Param
import pandas as pd
import os
import sys

sys.path.append("/tmp/")
import model

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
    data_loc = kwargs["params"]["data_path"]
    path = "/tmp/preped_data.csv"
    df = data_prep.load_data(data_loc)
    df['message'] = df['message'].apply(data_prep.basic_reg)
    df.to_csv(path)
    ti.xcom_push(key="cleaned_data", value={"path": path})

"""
# Model training function
def train_model(**kwargs):
    ti = kwargs["ti"]
    cleaned_data = ti.xcom_pull(task_ids="clean_data_task", key="cleaned_data")
    LoggingMixin().log.info(f"Cleaned data: {cleaned_data}")
    path = cleaned_data["path"]
    vector_path="/tmp/vectorizer.pickle"
    data = pd.read_csv(path, encoding='latin-1', na_values="")
    data.dropna(how='any',inplace=True,axis=0)
    LoggingMixin().log.info(f"loaded data:{data.head(1)}")
    model_path = text_classification.train_xgboost(data,vector_path)
    ti.xcom_push(key="trained_model_path", value=model_path)
    ti.xcom_push(key="vector_path", value= vector_path)

"""
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


def test_accuracy(**kwargs):
    output_path = kwargs["params"]["output_path"]
    outputdata = data_prep.load_data(output_path)
    model.model_accuracy(outputdata,logger=LoggingMixin())



# Get data function

# DAG definition
with DAG(
    "MVP_DAG",
    default_args=default_args,
    schedule_interval=None,
    params = {
    "model_path": Param(default="/tmp/model", type="string"),
    "model_name": Param(default="pathologyBERT_canvsnoncan", type="string"),
    "output_path": Param(default="/tmp/output/predicted_data.csv", type="string"),
    "data_path": Param(default="/tmp/input/test_df.csv", type="string")
}
) as dag:

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

    # Prediction task
    predict_task = PythonOperator(
        task_id="predict_model_task",
        python_callable=predict_data,
        provide_context=True,
        dag=dag,
    )
    test_accuracy_task = PythonOperator(
        task_id="test_accuracy_task",
        python_callable=test_accuracy,
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
    data_cleaning_task >> predict_task >> test_accuracy_task
