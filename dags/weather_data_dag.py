from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging
import json
import os

# Import required modules
from weather_data import fetch_all_cities_weather
from weather_spark_job import transform_data, create_spark_session, insert_into_mongo

logger = logging.getLogger('dag_logger')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Output directory setup
output_dir = r"/opt/airflow/data/output.json"
os.makedirs(output_dir, exist_ok=True)  # Create the directory if it doesn't exist

output_file = os.path.join(output_dir, "output.json")

# Weather API and Spark configurations for cities
cities = [
    {"name": "Hamburg", "lat": 53.55, "lon": 10.0},
    {"name": "Berlin", "lat": 52.52, "lon": 13.405},
    {"name": "Munich", "lat": 48.1351, "lon": 11.5820},
    {"name": "Cologne", "lat": 50.9375, "lon": 6.9603},
    {"name": "Frankfurt", "lat": 50.1109, "lon": 8.6821},
    {"name": "Stuttgart", "lat": 48.7758, "lon": 9.1829},
    {"name": "Düsseldorf", "lat": 51.2277, "lon": 6.7735},
    {"name": "Dortmund", "lat": 51.5136, "lon": 7.4653},
    {"name": "Essen", "lat": 51.4556, "lon": 7.0116},
    {"name": "Leipzig", "lat": 51.3397, "lon": 12.3731},
    {"name": "Bremen", "lat": 53.0793, "lon": 8.8017},
    {"name": "Dresden", "lat": 51.0504, "lon": 13.7373},
    {"name": "Hanover", "lat": 52.3759, "lon": 9.7320},
    {"name": "Nuremberg", "lat": 49.4521, "lon": 11.0767},
    {"name": "Duisburg", "lat": 51.4344, "lon": 6.7623},
    {"name": "Bochum", "lat": 51.4818, "lon": 7.2162},
    {"name": "Wuppertal", "lat": 51.2562, "lon": 7.1508},
    {"name": "Bielefeld", "lat": 52.0302, "lon": 8.5325},
    {"name": "Bonn", "lat": 50.7374, "lon": 7.0982},
    {"name": "Münster", "lat": 51.9607, "lon": 7.6261}
]

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 14),
    'catchup': False,
}

dag = DAG(
    'weather_data_processing_dag_new',
    default_args=default_args,
    description='A DAG to fetch and process weather data without XCom',
    schedule_interval=timedelta(days=1),
    max_active_runs=1
)

# Start job
def start_job():
    logging.info("Starting the weather data processing pipeline.")

# Fetch weather data and save as JSON file
def fetching_data_job():
    try:
        logging.info("Fetching weather data...")

        weather_data = fetch_all_cities_weather(cities)
        if weather_data:
            logging.info(f"Successfully fetched weather data for {len(weather_data)} cities.")

            with open(output_file, "w", encoding="utf-8") as f:
                for item in weather_data:
                    f.write(json.dumps(item) + "\n")
        else:
            logging.error("Failed to fetch weather data.")
            raise ValueError("No weather data fetched.")
    except Exception as e:
        logging.error(f"An error occurred while fetching weather data: {e}")
        raise

# Read and transform data, then insert into MongoDB
def transform_data_job():
    try:
        logging.info("Reading and transforming weather data from JSON...")

        with open(output_file, "r", encoding="utf-8") as f:
            weather_data = [json.loads(line) for line in f]

        spark = create_spark_session()  # Get or create Spark session
        rdd = spark.sparkContext.parallelize(weather_data)
        df = spark.read.json(rdd)

        # Transformation and insert into MongoDB
        transformed_df = transform_data(df)
        insert_into_mongo(transformed_df)
        logging.info("Weather data transformation complete.")
        logging.info("Weather data successfully inserted into MongoDB.")
    except Exception as e:
        logging.error(f"An error occurred during the transformation process: {e}")
        raise

# End job
def end_data_job():
    logging.info("Weather data processing pipeline finished.")

# Airflow Tasks
start_task = PythonOperator(
    task_id='start_job',
    python_callable=start_job,
    dag=dag
)

fetching_data_task = PythonOperator(
    task_id='fetching_data_job',
    python_callable=fetching_data_job,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data_job',
    python_callable=transform_data_job,
    dag=dag
)

end_task = PythonOperator(
    task_id='end_data_job',
    python_callable=end_data_job,
    dag=dag
)

# DAG task order
start_task >> fetching_data_task >> transform_data_task >> end_task
