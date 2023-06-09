# PYSPARK_NYC_TAXI

Just a simple demonstration of using PySpark. 

## Data

Download Yellow taxi trip records(PARQUET files) from [here](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and save it here:

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

## Run

To run the PySpark application, you would need Java 8 or a later version hence download the Java version from Oracle and install it on your system.

Post-installation set JAVA_HOME and PATH variable

```bash
JAVA_HOME = C:\Program Files\Java\jdk1.8.0_201
PATH = %PATH%;C:\Program Files\Java\jdk1.8.0_201\bin
```
Further, declare some enviroment variables, which are:

_PYSPARK_PYTHON_ is a path to your Python binary. It's very important for Workers to be set with that to run calculation stuff, overwise exception will be raised. 

```bash
PYSPARK_PYTHON=/home/user/projects/pyspark_nyc_taxi/venv/bin/python
```

or for Windows:

```bash
PYSPARK_PYTHON=c:\Users\user\projects\pyspark_nyc_taxi\venv\Scripts\python.exe
```

_HADOOP_HOME_ is a path to Hadoop directory. If you haven't installed Hadoop on your local machine use PySpark directory. So it could be like:

```bash
HADOOP_HOME=/home/user/projects/pyspark_nyc_taxi/venv/lib/python3.9/site-packages/pyspark
```

or for Windows:

```bash
HADOOP_HOME=c:\Users\user\projects\pyspark_nyc_taxi\venv\Lib\site-packages\pyspark
```

Also, if you run this on Windows you'll need to download _wintools.exe_ and copy it to %HADOOP_HOME%\bin folder. Winutils are different for each Hadoop version hence download the right version from [here](https://github.com/steveloughran/winutils).

