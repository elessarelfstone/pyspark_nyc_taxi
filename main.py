import os
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import (StructType, StructField, StringType,
                               TimestampType, IntegerType, DoubleType, LongType)


MYSQL_HOST = 'localhost'
MYSQL_PORT = 3306
MYSQL_DATABASE = 'data'
MYSQL_TABLE = 'pyspark_lab'
MYSQL_USER = 'user'
MYSQL_PASSWORD = 'my-secret'


dim_columns = ['id', 'name']

vendor_rows = [
    (1, 'Creative Mobile Technologies, LLC '),
    (2, 'VeriFone Inc'),
]

rates_rows = [
    (1, 'Standard rate'),
    (2, 'JFK'),
    (3, 'Newark'),
    (4, 'Nassau of Westchester'),
    (5, 'Negotiated fare'),
    (6, 'Group ride'),
]

payment_rows = [
    (1, 'Credit card'),
    (1, 'Cash'),
    (1, 'No charge'),
    (1, 'Standard rate'),
    (1, 'Dispute'),
    (1, 'Unknown'),
    (1, 'Voided trip'),
]

trips_schema = StructType([
    StructField('VendorID', LongType(), True),
    StructField('tpep_pickup_datetime', TimestampType(), True),
    StructField('tpep_dropoff_datetime', TimestampType(), True),
    StructField('Passenger_count', DoubleType(), True),
    StructField('Trip_distance', DoubleType(), True),
    StructField('RateCodeID', DoubleType(), True),
    StructField('Store_and_fwd_flag', StringType(), True),
    StructField('PULocationID', LongType(), True),
    StructField('DOLocationID', LongType(), True),
    StructField('Payment_type', LongType(), True),
    StructField('Fare_amount', DoubleType(), True),
    StructField('Extra', DoubleType(), True),
    StructField('MTA_tax', DoubleType(), True),
    StructField('Tip_amount', DoubleType(), True),
    StructField('Tolls_amount', DoubleType(), True),
    StructField('Improvement_surcharge', DoubleType(), True),
    StructField('Total_amount', DoubleType(), True),
    StructField('Congestion_Surcharge', DoubleType()),
])


def create_dict(spark: SparkSession, header: list[str], data: list):
    df = spark.createDataFrame(data=data, schema=header)
    return df


def save_to_mysql(host: str, port: int, db_name: str, username: str,
                  password: str, df: DataFrame, table_name: str):

    props = {
        'user': f'{username}',
        'password': f'{password}',
        'driver': 'com.mysql.cj.jdbc.Driver',
        'ssl': 'true',
        'sslmode': 'none'
    }

    df.write.jdbc(
        url=f'jdbc:mysql://{host}:{port}/{db_name}',
        table=table_name,
        properties=props
    )


def agg_calc(spark: SparkSession) -> DataFrame:

    data_path = Path.home() / 'data' / 'nyc_yellow_taxi'

    trip_fact = spark.read.schema(trips_schema).parquet(str(data_path))

    datamart = trip_fact \
        .where(trip_fact['VendorID'].isNotNull()) \
        .groupby(trip_fact['VendorID'],
                 trip_fact['Payment_type'],
                 trip_fact['RateCodeID'],
                 f.to_date(trip_fact['tpep_pickup_datetime']).alias('dt')
                 ) \
        .agg(f.sum(trip_fact['Total_amount']).alias('sum_amount'), f.avg(trip_fact['Tip_amount']).alias('avg_tips')) \
        .select(f.col('dt'),
                f.col('VendorID'),
                f.col('Payment_type'),
                f.col('RateCodeID'),
                f.col('sum_amount'),
                f.col('avg_tips')) \
        .orderBy(f.col('dt').desc(), f.col('VendorID'))

    return datamart


def main(spark: SparkSession):
    vendor_dim = create_dict(spark, dim_columns, vendor_rows)
    payment_dim = create_dict(spark, dim_columns, payment_rows)
    rates_dim = create_dict(spark, dim_columns, rates_rows)

    datamart = agg_calc(spark).cache()
    datamart.show(truncate=False, n=100)

    joined_datamart = datamart \
        .join(other=vendor_dim, on=vendor_dim['id'] == f.col('VendorID'), how='inner') \
        .join(other=payment_dim, on=payment_dim['id'] == f.col('Payment_type'), how='inner') \
        .join(other=rates_dim, on=rates_dim['id'] == f.col('RateCodeID'), how='inner') \
        .select(f.col('dt'),
                f.col('VendorID'), f.col('Payment_type'), f.col('RateCodeID'), f.col('sum_amount'),
                f.col('avg_tips'),
                rates_dim['name'].alias('rate_name'), vendor_dim['name'].alias('vendor_name'),
                payment_dim['name'].alias('payment_name')
                )

    joined_datamart.show(truncate=False, n=50)

    save_to_mysql(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        db_name=MYSQL_DATABASE,
        username=MYSQL_USER,
        password=MYSQL_PASSWORD,
        df=joined_datamart,
        table_name=f'{MYSQL_DATABASE}.{MYSQL_TABLE}'
    )
    # print('end')


if __name__ == '__main__':
    main(SparkSession
         .builder
         .config('spark.jars', './mysql-connector-java-8.0.25.jar')
         .appName('My first spark job')
         .getOrCreate())
