from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

from datetime import datetime, timedelta

from dotenv import load_dotenv
import os

from package.quality_checks.quality_checks import check_silver_data


load_dotenv()


BUCKET_NAME = os.getenv("BUCKET_NAME")

EMR_APPLICATION_ID = os.getenv(
    "EMR_APPLICATION_ID"
)

EMR_EXECUTION_ROLE_ARN = os.getenv(
    "EMR_EXECUTION_ROLE_ARN"
)

REDSHIFT_ROLE_ARN = os.getenv(
    "REDSHIFT_ROLE_ARN"
)

REDSHIFT_WORKGROUP = os.getenv(
    "REDSHIFT_WORKGROUP"
)

REDSHIFT_DATABASE = os.getenv(
    "REDSHIFT_DATABASE",
    "dev"
)


default_args = {
    "owner": "julia",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}


with DAG(
    dag_id="telecom_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 8, 5),
    schedule=None,
    catchup=False,
    tags=["telecom", "spark", "aws"]
) as dag:


    bronze_to_silver = EmrServerlessStartJobOperator(
        task_id="bronze_to_silver_job",

        application_id=EMR_APPLICATION_ID,

        execution_role_arn=EMR_EXECUTION_ROLE_ARN,

        job_driver={
            "sparkSubmit": {

                "entryPoint": (
                    f"s3://{BUCKET_NAME}/"
                    "scripts/bronze_to_silver_job.py"
                ),

                "sparkSubmitParameters":
                    "--conf spark.executor.instances=3 "
                    "--py-files "
                    f"s3://{BUCKET_NAME}/"
                    "scripts/package.zip"
            }
        }
    )


    silver_quality_check = PythonOperator(
        task_id="silver_quality_check",
        python_callable=check_silver_data
    )


    silver_to_gold = EmrServerlessStartJobOperator(
        task_id="silver_to_gold_job",

        application_id=EMR_APPLICATION_ID,

        execution_role_arn=EMR_EXECUTION_ROLE_ARN,

        job_driver={
            "sparkSubmit": {

                "entryPoint": (
                    f"s3://{BUCKET_NAME}/"
                    "scripts/silver_to_gold_job.py"
                ),

                "sparkSubmitParameters":
                    "--conf spark.executor.instances=3 "
                    "--py-files "
                    f"s3://{BUCKET_NAME}/"
                    "scripts/package.zip"
            }
        }
    )


    load_to_redshift = RedshiftDataOperator(
        task_id="load_to_redshift",

        sql=f"""

        TRUNCATE TABLE city_summary;

        COPY city_summary
        FROM 's3://{BUCKET_NAME}/gold/city_summary/'
        IAM_ROLE '{REDSHIFT_ROLE_ARN}'
        FORMAT AS PARQUET;


        TRUNCATE TABLE network_summary;

        COPY network_summary
        FROM 's3://{BUCKET_NAME}/gold/network_summary/'
        IAM_ROLE '{REDSHIFT_ROLE_ARN}'
        FORMAT AS PARQUET;


        TRUNCATE TABLE device_summary;

        COPY device_summary
        FROM 's3://{BUCKET_NAME}/gold/device_summary/'
        IAM_ROLE '{REDSHIFT_ROLE_ARN}'
        FORMAT AS PARQUET;


        TRUNCATE TABLE daily_summary;

        COPY daily_summary
        FROM 's3://{BUCKET_NAME}/gold/daily_summary/'
        IAM_ROLE '{REDSHIFT_ROLE_ARN}'
        FORMAT AS PARQUET;

        """,

        database=REDSHIFT_DATABASE,

        workgroup_name=REDSHIFT_WORKGROUP
    )


    bronze_to_silver >> silver_quality_check >> silver_to_gold >> load_to_redshift