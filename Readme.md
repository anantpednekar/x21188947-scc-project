# scalable cloud computing -project

## The data set

BGL is an open data set of logs collected from a BlueGene/L supercomputer at Lawrence Livermore National Labs. It is equipped with 131,072 processors and 32,768GB memory.

The log file can be downloaded from Zenodo1. A sample line from the log file is shown below. 

```
- 1121707460 2005.07.18 R23-M1-N0-C:J05-U01 2005-07-18-10.24.20.440509 R23-M1-N0-C:J05-U01 RAS KERNEL INFO generating core.7663
```
This can be parsed as below.

| Value           | Interpretation        |
|-----------------|-----------------------|
| -               | Alert message flag    |
| 1121707460      | Timestamp             |
| 2005.07.18      | Date                  |
| R23-M1-N0-C:J05-U01 | Node             |
| 2005-07-18-10.24.20.440509 | Date and Time |
| R23-M1-N0-C:J05-U01 | Node (repeated)  |
| RAS             | Message Type          |
| KERNEL          | System Component      |
| INFO            | Level                 |
| generating core.7663 | Message Content   |

## Dataset Link

You can download the BGL dataset from the following link: [BGL Dataset (Tar.gz)](https://zenodo.org/record/3227177/files/BGL.tar.gz)

## Data Processing Requirements

### Requirement 1: BGL.log in Hadoop system
- The system should have access to the `BGL.log` file located in the Hadoop system.

### Requirement 2: BGL.log in the folder for Spark RDD
- The `BGL.log` file should be available in a designated folder for Spark Resilient Distributed Datasets (RDD).

### Requirement 3: BGL.csv for SparkSQL
- A CSV file named `BGL.csv` is required for use with SparkSQL.

## Command for Converting BGL log to CSV

To convert the `BGL.log` file to CSV format, you can use the following `awk` command:

```bash
awk '{for(i=1;i<10;i++)sub(" ",",")}1' BGL.log > BGL.csv
```


# Running Data Processing Codes

To process the BGL.log dataset, follow these steps:

```bash
# Step 1: Running Q12Hadoop.py
./Q12Hadoop.py -r hadoop hdfs:////bglinput/BGL.log > Log_Q12_Hadoop.log

# Step 2: Running Q14Hadoop.py
./Q14Hadoop.py -r hadoop hdfs:////bglinput/BGL.log > Log_Q14_Hadoop.log

# Step 3: Running Q19Hadoop.py
./Q19Hadoop.py -r hadoop hdfs:////bglinput/BGL.log > Log_Q19_Hadoop.log

# Step 4: Running Q19SparkRDD.py
./Q19SparkRDD.py > Log_Q19_SparkRDD.log

# Step 5: Running Q19SparkSQL.py
./Q19SparkSQL.py > Log_Q19_SparkSQL.log

# Step 6: Running Q3Hadoop.py
./Q3Hadoop.py -r hadoop hdfs:////bglinput/BGL.log > Log_Q3_Hadoop.log

# Step 7: Running Q3SparkRDD.py
./Q3SparkRDD.py > Log_Q3_SparkRDD.log

# Step 8: Running Q6Hadoop.py
./Q6Hadoop.py -r hadoop hdfs:////bglinput/BGL.log > Log_Q6_Hadoop.log

# Step 9: Running extra_Hadoop.py
./extra_Hadoop.py -r hadoop hdfs:////bglinput/BGL.log > Log_extra_Hadoop.log
```