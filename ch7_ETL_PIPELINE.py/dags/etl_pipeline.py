from airflow.sdk import dag, task
import pandas as pd
import requests
import os
from sqlalchemy import create_engine,text
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag()
def etl_pipeline():
    
    @task()
    def timestamp():
        from datetime import datetime
        return datetime.now().isoformat()
    
    @task()
    def extract(ti):
        timestamp_value = ti.xcom_pull(task_ids="timestamp", key="return_value")
        
        url = "http://fastapi:8000/fetch_data"
        response = requests.get(url)
        data = response.json().get("data", [])
        
        os.makedirs("/tmp/raw", exist_ok=True)
        
        with open(f"/tmp/raw/data_{timestamp_value}.csv", "w") as f:
            f.write("id,name,age\n")
            for item in data:
                f.write(f"{item['id']},{item['name']},{item['age']}\n")
        
        return f"Data extracted from staging area and saved to /tmp/raw/data_{timestamp_value}.csv"
    
    @task()
    def transform(ti):
        timestamp_value = ti.xcom_pull(task_ids="timestamp", key="return_value")
        
        df = pd.read_csv(f"/tmp/raw/data_{timestamp_value}.csv")
        df["added_age"] = df["age"] + 5
        os.makedirs("/tmp/transformed", exist_ok=True)
        df.to_csv(f"/tmp/transformed/data_{timestamp_value}.csv", index=False)
        return f"Data transformed and saved to /tmp/transformed/data_{timestamp_value}.csv"
    
    @task()
    def create_table():
        query = """ create table if not exists employees (
                    id int,
                    name varchar(255),
                    age int,
                    added_age int
                ); """
                
        conn = create_engine("postgresql://airflow:airflow@postgres:5432/airflow").connect()
        with conn.begin() as transaction:
            try:
                conn.execute(text(query))
            except Exception as e:          
                transaction.rollback()
                raise e
            else:
                transaction.commit()
                
    @task()
    def load(ti):
        timestamp_value = ti.xcom_pull(task_ids="timestamp", key="return_value")
        
        df = pd.read_csv(f"/tmp/transformed/data_{timestamp_value}.csv")
        engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")
        df.to_sql("employees", con=engine, if_exists="append", index=False)
        
        engine.dispose()

        
    create_table_new = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="my_postgresql",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT,
            name VARCHAR(255),
            age INT,
            added_age INT
        );
        """
    )
    
    @task
    def write_to_new_table(ti):
        timestamp_value = ti.xcom_pull(task_ids="timestamp", key="return_value")
        hook = PostgresHook(postgres_conn_id="my_postgresql")
        hook.copy_expert(
            sql="COPY users(id, name, age, added_age) FROM STDIN WITH CSV HEADER",
            filename=f"/tmp/transformed/data_{timestamp_value}.csv"
        )
        
    create_table = create_table()
            
    timestamp() >>  extract() >> transform() >> [create_table,create_table_new]
    create_table >> load() 
    create_table_new >> write_to_new_table()

etl_pipeline()