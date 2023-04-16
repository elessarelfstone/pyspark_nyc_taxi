# PYSPARK_NYC_TAXI

Just a simple demonstration of using PySpark. 

## Data

Download Yellow taxi trip records(PARQUET files) from [here](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and save it to here:

```python
Path.home() / 'data' / 'nyc_yellow_taxi'
```

Get working instance of MySQL and create stand to store output data using following params and credentials:

```python
MYSQL_HOST = 'localhost'
MYSQL_PORT = 3306
MYSQL_DATABASE = 'data'
MYSQL_TABLE = 'pyspark_lab'
MYSQL_USER = 'user'
MYSQL_PASSWORD = 'my-secret'
```
To run this project you have to declare some enviroment variables, which are:

_PYSPARK_PYTHON_ is a path to your Python binary. It's very important for Workers to be set with that to run calculation stuff, overwise exception will be raised.

HADOOP_HOME

If you use virtualenv add env variables(in bash or pycharm)
PYSPARK_PYTHON = path to python in venv
HADOOP_HOME = path to pyspark lib in venv