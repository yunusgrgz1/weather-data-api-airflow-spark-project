from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_unixtime, date_format
import json
import logging
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO)

client = MongoClient(
    "mongodb://root:example@mongodb:27017/",
    username="root",
    password="example",
    authSource="admin"  # Authentication DB (usually 'admin')
)

# Select database
db = client.weather_data

# Select collection (will be created if it doesn't exist)
collection = db.daily_weather

spark = None

def create_spark_session():
    """
    If there is no global SparkSession, start a new one.
    """
    global spark
    if spark is None:
        spark = SparkSession.builder \
            .appName("Weather Data Processing") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()
    return spark


def fetch_weather_data(cities):
    """
    Fetches weather data for given cities and converts it into a Spark DataFrame.
    """
    try:
        logging.info("Fetching weather data for cities: %s", cities)
        all_weather_data = fetch_all_cities_weather(cities)

        if all_weather_data:
            logging.info("Weather data fetched successfully. Converting to Spark DataFrame...")
            spark = create_spark_session()

            rdd = spark.sparkContext.parallelize(all_weather_data)
            df = spark.read.json(rdd)
            logging.info("Data successfully converted to DataFrame.")
            return df
        else:
            logging.error("No weather data to convert into DataFrame.")
            return None

    except Exception as e:
        logging.exception(f"Error while fetching or processing weather data: {e}")
        return None


def transform_data(df):
    """
    Performs transformations on the raw weather data DataFrame.
    """
    logging.info("Transforming weather data...")

    df_selected = df.select(
        col("city"),
        col("lat"),
        col("lon"),
        col("timezone"),
        col("current.dt").alias("timestamp"),
        col("current.sunrise"),
        col("current.sunset"),
        col("current.temp"),
        col("current.feels_like"),
        col("current.pressure"),
        col("current.humidity"),
        col("current.dew_point"),
        col("current.uvi"),
        col("current.clouds"),
        col("current.visibility"),
        col("current.wind_speed"),
        col("current.wind_deg"),
        explode(col("current.weather")).alias("weather")
    )

    df = df_selected.select(
        "*",
        col("weather.main").alias("weather_main"),
        col("weather.description").alias("weather_description")
    )

    transformed_df = df.withColumn(
        "timestamp",
        date_format(from_unixtime(col("timestamp")), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "sunrise",
        date_format(from_unixtime(col("sunrise")), "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "sunset",
        date_format(from_unixtime(col("sunset")), "yyyy-MM-dd HH:mm:ss")
    ).drop("weather")

    logging.info("Transformation complete.")
    return transformed_df


def insert_into_mongo(transformed_df):
    """
    Inserts transformed weather data into MongoDB.
    """
    try:
        json_data = transformed_df.toJSON().collect()
        parsed_data = [json.loads(record) for record in json_data]
        
        if parsed_data:
            collection.insert_many(parsed_data)
            logging.info("All data successfully inserted into MongoDB!")
        else:
            logging.warning("No data found to insert.")

    except Exception as e:
        logging.error(f"Error occurred while inserting data into MongoDB: {e}")
