# IoT-Assignment-Real-time-Stream-Processing

## Hospital Patient Monitoring with Spark Structured Streaming

This project implements a real-time stream processing pipeline using Apache Spark Structured Streaming.

The chosen scenario: Hospital Patient Monitoring. The system reads patient heart rate data from a watched directory and detects sustained elevated heart rate alerts.

The alert condition is:

A patient triggers an alert if their average heart rate is greater than 100 bpm in two consecutive 2-minute windows.

### Dataset

The dataset used in this project is:
Hospital Patient Monitoring Dataset which is available on Kaggle. The original dataset contains multiple patient health-related columns, including heart rate, SpO2 level, blood pressure, body temperature, fall detection, predicted disease, and alert-related columns.

For this implementation, the following columns are used:

Patient Number
Heart Rate (bpm)

These columns are cleaned and renamed as:

patient_number
heart_rate

The original dataset does not include a timestamp column. Since Spark Structured Streaming window operations require an event-time column, synthetic timestamps were generated in create_batches.py.

## Project Structure

```text
streaming_job.py                  Main Spark Structured Streaming job
create_batches.py                 Simulates streaming by writing CSV batches
requirements.txt                  Dependencies
README.md                         Project documentation
alert_screenshot.jpg              Screenshot of the alert output
dataset/                          Contains the Excel dataset
stream_input/                     Watched directory for streaming CSV files

```

## Requirements

This project was tested using **Python 3.11** virtual environment. 

Check the Python version using:

```bash
python --version
```

Expected version:

```text
Python 3.11.x
```

Java is also required because PySpark runs on the JVM. This project assumes that Java is already installed on the machine.

Check Java using:

```bash
java -version
```

The required Python packages are listed in `requirements.txt`:

```text
pyspark==3.5.0
pandas
openpyxl
```

## How to run using Git Bash

### On Terminal 1:

First, clone the repository to your machine and Go to the project folder. Inside the project folder:

Install the dependencies:

```bash
pip install -r requirements.txt
```

To confirm the installed PySpark version:

```bash
pip show pyspark
```

Expected version:

```text
Version: 3.5.0
```


Run the Spark streaming job:

```bash
python streaming_job.py
```

Expected output:

```text
Hospital patient monitoring stream started.
Waiting for clinical alerts...
```

Leave this terminal running.

### Terminal 2

Run the batch generator:

```bash
python create_batches.py
```

This writes CSV files into the `stream_input/` directory one batch at a time.

## Running on Windows

On Windows, PySpark might require Hadoop utilities when reading local folders using `readStream`.

The following files should exist:

```text
C:\hadoop\bin\winutils.exe
C:\hadoop\bin\hadoop.dll
```

Before running `streaming_job.py`, run these commands in Git Bash:

```bash
export HADOOP_HOME=/c/hadoop
export PATH=$PATH:/c/hadoop/bin
export SPARK_SUBMIT_OPTS="-Dhadoop.home.dir=C:/hadoop"
```

Then run the streaming job:

```bash
python streaming_job.py
```

This is a Windows-specific environment setup issue. The same project can run on Linux without the `winutils.exe` and `hadoop.dll` workaround.




### Why this window type was chosen


A 2-minute window is short enough to detect abnormal heart rate patterns quickly, but it also reduces false alerts caused by a single temporary spike. Since the alert condition checks two consecutive windows, the system focuses on sustained elevated heart rate rather than one isolated reading.


### Where the pipeline requires state

The pipeline requires state in two main places.

First, Spark maintains state for the 2-minute window aggregation. For each patient and each 2-minute window, Spark has to keep the heart rate readings until it can calculate the average heart rate for that window.

Second, the alert logic requires state when comparing the current window with the previous window for the same patient. 

