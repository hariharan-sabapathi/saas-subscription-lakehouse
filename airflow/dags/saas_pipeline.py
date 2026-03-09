from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

default_args = {
    "owner": "hari",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="saas_lakehouse_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    with TaskGroup("bronze_layer") as bronze_layer:

        bronze_users = BashOperator(
            task_id="bronze_users",
            bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
        --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        --conf spark.jars.ivy=/tmp/.ivy \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        /opt/spark/work-dir/spark/bronze_users.py
        """,
            dag=dag
        )
        
        bronze_subscriptions = BashOperator(
            task_id="bronze_subscriptions",
            bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
        --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        --conf spark.jars.ivy=/tmp/.ivy \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        /opt/spark/work-dir/spark/bronze_subscriptions.py
        """,
            dag=dag
        )

        bronze_payments = BashOperator(
            task_id="bronze_payments",
            bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
        --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        --conf spark.jars.ivy=/tmp/.ivy \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        /opt/spark/work-dir/spark/bronze_payments.py
        """,
            dag=dag
        )

        bronze_invoices = BashOperator(
            task_id="bronze_invoices",
            bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
        --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        --conf spark.jars.ivy=/tmp/.ivy \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        /opt/spark/work-dir/spark/bronze_invoices.py
        """,
            dag=dag
        )

    with TaskGroup("silver_layer") as silver_layer:

        silver_users = BashOperator(
            task_id="silver_users",
            bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
        --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        --conf spark.jars.ivy=/tmp/.ivy \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        /opt/spark/work-dir/spark/silver_users.py
        """,
            dag=dag
        )

        silver_subscriptions = BashOperator(
            task_id="silver_subscriptions",
            bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
        --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        --conf spark.jars.ivy=/tmp/.ivy \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        /opt/spark/work-dir/spark/silver_subscriptions.py
        """,
            dag=dag
        )

        silver_payments = BashOperator(
            task_id="silver_payments",
            bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
        --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        --conf spark.jars.ivy=/tmp/.ivy \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        /opt/spark/work-dir/spark/silver_payments.py
        """,
            dag=dag
        )

        silver_invoices = BashOperator(
            task_id="silver_invoices",
            bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
        --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        --conf spark.jars.ivy=/tmp/.ivy \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        /opt/spark/work-dir/spark/silver_invoices.py
        """,
            dag=dag
        )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd /opt/airflow/dbt_project && dbt run
        """
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        cd /opt/airflow/dbt_project && dbt test
        """
    )

    dbt_run >> dbt_test

