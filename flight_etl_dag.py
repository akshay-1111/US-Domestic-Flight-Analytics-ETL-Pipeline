from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
import plotly.express as px
import pandas as pd
import psycopg2
import os

# Constants
RAW_CRAWLER_NAME = "flight-etl-crawler"
PROCESSED_CRAWLER_NAME = "flight-etl-processed"
GLUE_JOB_NAME = "flight_transform_job"
REDSHIFT_ROLE = "arn:aws:iam::889351495092:role/redshift-role"
S3_PROCESSED_PATH = "s3://flights-etl/processed/us_flights/"
REDSHIFT_TABLE = "us_flights"
CHART_OUTPUT_PATH = "/tmp/delay_chart.html"
S3_BUCKET = "flights-etl"
S3_CHART_KEY = "visuals/delay_chart.html"
AWS_REGION = "ap-south-1"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    dag_id='flight_etl_pipeline',
    default_args=default_args,
    description='Flight ETL pipeline with Glue, Redshift, and Plotly',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['flights']
) as dag:

    def run_crawler(crawler_name):
        glue = AwsBaseHook(aws_conn_id='aws_default', client_type='glue').get_conn()
        try:
            glue.start_crawler(Name=crawler_name)
            print(f"Started crawler: {crawler_name}")
        except glue.exceptions.CrawlerRunningException:
            print(f"Crawler already running: {crawler_name}")
        except Exception as e:
            print(f"Failed to start crawler: {str(e)}")

    run_raw_crawler = PythonOperator(
        task_id='run_raw_crawler',
        python_callable=run_crawler,
        op_args=[RAW_CRAWLER_NAME]
    )

    run_glue_job = GlueJobOperator(
        task_id='run_glue_job',
        job_name=GLUE_JOB_NAME,
        region_name=AWS_REGION,
        aws_conn_id='aws_default'
    )

    run_processed_crawler = PythonOperator(
        task_id='run_processed_crawler',
        python_callable=run_crawler,
        op_args=[PROCESSED_CRAWLER_NAME]
    )

    copy_to_redshift = RedshiftDataOperator(
        task_id="copy_to_redshift",
        sql=f"""
            COPY public.{REDSHIFT_TABLE}
            FROM '{S3_PROCESSED_PATH}'
            IAM_ROLE '{REDSHIFT_ROLE}'
            FORMAT AS PARQUET;
        """,
        database="dev",
        cluster_identifier="redshift-cluster-1",
        aws_conn_id="aws_default"
    )

    create_views = RedshiftDataOperator(
        task_id="create_views",
        sql=[
            """
            DROP VIEW IF EXISTS delay_recovery_efficiency;
            CREATE VIEW delay_recovery_efficiency AS
            SELECT
              origin_name,
              dest_name,
              COUNT(*) AS total_flights,
              ROUND(AVG(dep_delay), 1) AS avg_dep_delay,
              ROUND(AVG(arr_delay), 1) AS avg_arr_delay,
              ROUND(AVG(dep_delay - arr_delay), 1) AS recovery_efficiency
            FROM public.us_flights
            GROUP BY origin_name, dest_name;
            """,
            """
            DROP VIEW IF EXISTS most_delayed_routes;
            CREATE VIEW most_delayed_routes AS
            SELECT
              origin,
              dest,
              COUNT(*) AS flights_per_route,
              ROUND(AVG(arr_delay), 1) AS avg_arr_delay
            FROM public.us_flights
            GROUP BY origin, dest
            ORDER BY avg_arr_delay DESC
            LIMIT 10;
            """,
            """
            DROP VIEW IF EXISTS origin_airport_delay;
            CREATE VIEW origin_airport_delay AS
            SELECT
              origin_name,
              COUNT(*) AS total_flights,
              ROUND(AVG(dep_delay), 1) AS avg_dep_delay,
              ROUND(AVG(arr_delay), 1) AS avg_arr_delay
            FROM public.us_flights
            GROUP BY origin_name
            ORDER BY avg_arr_delay DESC;
            """
        ],
        database="dev",
        cluster_identifier="redshift-cluster-1",
        aws_conn_id="aws_default"
    )

    def generate_plotly_chart():
        airflow_conn = BaseHook.get_connection("redshift_default")
        conn = psycopg2.connect(
            host=airflow_conn.host,
            port=airflow_conn.port,
            dbname=airflow_conn.schema,
            user=airflow_conn.login,
            password=airflow_conn.password
        )

        # 1️⃣ Delay Recovery Efficiency Chart
        query1 = """
            SELECT origin_name, dest_name, recovery_efficiency
            FROM delay_recovery_efficiency
            ORDER BY recovery_efficiency DESC
            LIMIT 10;
        """
        df1 = pd.read_sql(query1, conn)
        df1 = df1.sort_values(by='recovery_efficiency', ascending=True) 
        fig1 = px.bar(
            df1,
            x='origin_name',
            y='recovery_efficiency',
            color='dest_name',
            title='Top 10 Delay Recovery Routes',
            barmode='stack'
        )

        fig1.update_layout(xaxis={'categoryorder':'total descending'})

        # 2️⃣ Most Delayed Routes Chart
        query2 = """
            SELECT origin, dest, avg_arr_delay
            FROM most_delayed_routes;
        """
        df2 = pd.read_sql(query2, conn)
        df2 = df2.sort_values(by='avg_arr_delay', ascending=True)  # ✅ Sort descending
        fig2 =  px.bar(
            df2,
            x='origin',
            y='avg_arr_delay',
            color='dest',
            title='Most Delayed Routes by Arrival Delay',
            barmode='stack'
        )

        fig2.update_layout(xaxis={'categoryorder':'total descending'})
              

        # 3️⃣ Origin Airport Delay Chart
        query3 = """
            SELECT origin_name, avg_dep_delay, avg_arr_delay
            FROM origin_airport_delay;
        """
        df3 = pd.read_sql(query3, conn)
        fig3 = px.scatter(df3, x='avg_dep_delay', y='avg_arr_delay', text='origin_name',
                          title='Airport Delays: Departure vs Arrival',
                          labels={'avg_dep_delay': 'Avg Departure Delay (min)', 
                                 'avg_arr_delay': 'Avg Arrival Delay (min)'})

        # Write all charts into a single HTML file
        os.makedirs(os.path.dirname(CHART_OUTPUT_PATH), exist_ok=True)
        with open(CHART_OUTPUT_PATH, 'w') as f:
            f.write('<div style="text-align:center;"><h2>Top 10 Delay Recovery Routes</h2></div><br>')
            f.write(fig1.to_html(full_html=False, include_plotlyjs='cdn'))

            f.write('<hr><div style="text-align:center;"><h2>Most Delayed Routes</h2></div><br>')
            f.write(fig2.to_html(full_html=False, include_plotlyjs=False))

            f.write('<hr><div style="text-align:center;"><h2>Origin Airport Delays</h2></div><br>')
            f.write(fig3.to_html(full_html=False, include_plotlyjs=False))

        

        conn.close()

    plotly_task = PythonOperator(
        task_id='generate_plotly_chart',
        python_callable=generate_plotly_chart
    )

    def upload_chart_to_s3():
        hook = S3Hook(aws_conn_id='aws_default')
        hook.load_file(
            filename=CHART_OUTPUT_PATH,
            key=S3_CHART_KEY,
            bucket_name=S3_BUCKET,
            replace=True
        )
        print(f"✅ Chart uploaded to s3://{S3_BUCKET}/{S3_CHART_KEY}")

    upload_chart_task = PythonOperator(
        task_id='upload_chart_to_s3',
        python_callable=upload_chart_to_s3
    )

    # DAG dependencies
    run_raw_crawler >> run_glue_job >> run_processed_crawler
    run_processed_crawler >> copy_to_redshift >> create_views >> plotly_task >> upload_chart_task