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

```bash
HADOOP_HOME=/home/user/projects/pyspark_nyc_taxi/venv/bin/python
```

or for Windows:

```bash
HADOOP_HOME=c:\Users\user\projects\pyspark_nyc_taxi\venv\Scripts\python.exe
```

_HADOOP_HOME_ is a path to Hadoop directory. If you haven't installed Hadoop on your local machine use the a PySpark directory. So it could be like:

```bash
HADOOP_HOME=/home/user/projects/pyspark_nyc_taxi/venv/lib/python3.9/site-packages/pyspark
```

or for Windows:

```bash
HADOOP_HOME=c:\Users\user\projects\pyspark_nyc_taxi\venv\Lib\site-packages\pyspark
```
