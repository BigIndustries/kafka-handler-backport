# Kafka Storage Handler Module

## Important
This storage handler for Apache Kafka has been backported to Hive 1.2.1 and Hadoop 2.7.3. As such functionality such as vectorized queries and some unsupported datatypes have been removed from the codebase.

All unit tests pass, none have been disabled nor have any been removed from the test suite.

Some future code has been introduced to ensure solution continuity. Currently the code uses Kafka 2.x client, which appears to be fully backwards compatible to Kafka 1.x.

### Known limitations
Hive 1.2.1 has no commitInsertTable MetaHook feature. This means that only optimistic commit semantics are supported for EXACTLY_ONCE writes to Kafka in this backport. Therefore `hive.kafka.optimistic.commit` has been enabled by default.

Disabling optimistic commit semantics is not recommended, as records will be written to the Kafka topic, but 2PC will never be effected and the records will not be readable within a `read_committed` scope. "Optimistic commit" commits all succesfully written records within the scope of a given task upon completion of the task. Thus, if 5 tasks are required to complete the Hive query, each task will commit a part of the dataset upon completion, and there will therefore be 5 commits to Kafka. Should a task fail for some reason, the transaction for the given task will be aborted, and the task may be retried, or it may be failed along with the job. Thus, if a task is failed, it may be possible that a part of the dataset of the given query is written to kafka, but not all of it.

This scenario, if not defended against, could lead to a partial/inconsistent state in Kafka, which may not be acceptable. In that case merge semantics could be used when pushing back records to Kafka, or deduplication logic could be used by the consumer in order to eliminate any duplicate records.

### Demo Usage Script
```sh
# build the backported hive-kafka storage handler
# all enabled unit tests should pass...some future
# language feature tests have been set to @Ignore
git clone https://github.com/BigIndustries/kafka-handler-backport.git
cd kafka-handler-backport
mvn clean test package

docker pull hrushikesh198/hive:1.0.0
docker pull michaeldqin/kafka

docker run --rm -d -p 2181:2181 -p 9092:9092 \
    --env ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 \
    --name kafka -h kafka michaeldqin/kafka

docker run -d -v ~/work:/root/work --name hive_cont1 --link kafka hrushikesh198/hive:1.0.0

# wait for services up
# docker logs -f hive_cont1
sudo cp target/kafka-handler-backport-1.0-SNAPSHOT.jar ~/work

docker run --rm -it --name producer --link kafka michaeldqin/kafka \
    kafka-topics.sh --create --zookeeper kafka:2181 --topic test-topic \
     --partitions 5 --replication-factor 1

docker exec -it hive_cont1 'bash'
yum install -y java-1.8.0-openjdk-headless #yep, j1.7 by default
hadoop fs -put /root/work/kafka-handler-backport-1.0-SNAPSHOT.jar /tmp/
echo "hi" > /tmp/file.csv
hadoop fs -mkdir /tmp/testdata
hadoop fs -put /tmp/file.csv /tmp/testdata
cp /root/work/kafka-handler-backport-1.0-SNAPSHOT.jar /usr/local/hive/lib/
unlink /usr/java/default/bin/java
ln -s /usr/bin/java /usr/java/default/bin/java
kill -9 $(ps aux | grep HiveServer2 | awk '{print $2}')
/usr/local/hive/bin/hiveserver2 &

beeline
!connect jdbc:hive2://localhost:10000/default
admin
admin
ADD JAR hdfs:///tmp/kafka-handler-backport-1.0-SNAPSHOT.jar;
CREATE EXTERNAL TABLE kafka_table (hello_world STRING) STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler' TBLPROPERTIES ('kafka.topic' = 'test-topic','kafka.bootstrap.servers' = 'kafka:9092');
INSERT INTO TABLE kafka_table VALUES('hi', null, null, -1, -1);
SELECT * FROM kafka_table;
# OK
# hi      NULL    1       0       1582933429914
# Time taken: 6.046 seconds, Fetched: 1 row(s)
# hive>

CREATE EXTERNAL TABLE test_data (hello_world STRING) STORED AS TEXTFILE LOCATION
'hdfs:///tmp/testdata/';

CREATE EXTERNAL TABLE kafka_table2 (
hello_world STRING,
block_offset_in_file BIGINT,
input_filename STRING
) 
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES (
'kafka.topic'='test-topic2',
'kafka.bootstrap.servers'='kafka:9092',
'kafka.serde.class'='org.apache.hadoop.hive.serde2.avro.AvroSerDe',
'kafka.write.semantic'='EXACTLY_ONCE'
);

INSERT INTO kafka_table2 
SELECT hello_world, 
BLOCK__OFFSET__INSIDE__FILE, 
INPUT__FILE__NAME, 
null, 
null, 
-1, 
-1 
FROM test_data;

SELECT * FROM kafka_table2 order by input_filename, block_offset_in_file;
!quit
```

------

Storage Handler that allows users to connect/analyze/transform Kafka topics.
The workflow is as follows:
- First, the user will create an external table that is a view over one Kafka topic
- Second, the user will be able to run any SQL query including write back to the same table or different Kafka backed table

## Kafka Management

Kafka Java client version: 2.x

This handler does not commit offsets of topic partition reads either using the intrinsic Kafka capability or in an external
storage.  This means a query over a Kafka topic backed table will be a full topic read unless partitions are filtered
manually, via SQL, by the methods described below. In the ETL section, a method for storing topic offsets in Hive tables
is provided for tracking consumer position but this is not a part of the handler itself.

## Usage

### Create Table
Use the following statement to create a table:

```sql
CREATE EXTERNAL TABLE 
  kafka_table (
    `timestamp` TIMESTAMP,
    `page` STRING,
    `newPage` BOOLEAN,
    `added` INT, 
    `deleted` BIGINT,
    `delta` DOUBLE)
STORED BY 
  'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES ( 
  "kafka.topic" = "test-topic",
  "kafka.bootstrap.servers" = "localhost:9092");
```

The table property `kafka.topic` is the Kafka topic to connect to and `kafka.bootstrap.servers` is the Kafka broker connection string.
Both properties are mandatory.
On the write path if such a topic does not exist the topic will be created if Kafka broker admin policy allows for 
auto topic creation.

By default the serializer and deserializer is JSON, specifically `org.apache.hadoop.hive.serde2.JsonSerDe`.

If you want to change the serializer/deserializer classes you can update the TBLPROPERTIES with SQL syntax `ALTER TABLE`.

```sql
ALTER TABLE 
  kafka_table 
SET TBLPROPERTIES (
  "kafka.serde.class" = "org.apache.hadoop.hive.serde2.avro.AvroSerDe");
```
 
List of supported serializers and deserializers:

|Supported Serializers and Deserializers|
|-----|
|org.apache.hadoop.hive.serde2.JsonSerDe|
|org.apache.hadoop.hive.serde2.OpenCSVSerde|
|org.apache.hadoop.hive.serde2.avro.AvroSerDe|
|org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe|
|org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe|

#### Table Definitions 
In addition to the user defined column schema, this handler will append additional columns allowing
the user to query the Kafka metadata fields:
- `__key` Kafka record key (byte array)
- `__partition` Kafka record partition identifier (int 32)
- `__offset` Kafka record offset (int 64)
- `__timestamp` Kafka record timestamp (int 64)

 
### Query Table

List the table properties and all the partition/offsets information for the topic. 
```sql
DESCRIBE EXTENDED 
  kafka_table;
```

Count the number of records where the record timestamp is within the last 10 minutes of query execution time.

```sql
SELECT 
  COUNT(*)
FROM 
  kafka_table 
WHERE 
  `__timestamp` >  1000 * TO_UNIX_TIMESTAMP(CURRENT_TIMESTAMP - INTERVAL '10' MINUTES);
```

The storage handler allows these metadata fields to filter push-down read optimizations to Kafka.
For instance, the query above will only read the records with timestamp satisfying the filter predicate. 
Please note that such time based filtering (Kafka consumer partition seek) is only viable if the Kafka broker 
version allows time based look up (Kafka 0.11 or later versions)

In addition to **time based filtering**, the storage handler reader is able to filter based on a
particular partition offset using the SQL WHERE clause.
Currently supports operators `OR` and `AND` with comparison operators `<`, `<=`, `>=`, `>`. 

#### Metadata Query Examples

```sql
SELECT
  COUNT(*)
FROM 
  kafka_table
WHERE 
  (`__offset` < 10 AND `__offset` >3 AND `__partition` = 0)
  OR 
  (`__partition` = 0 AND `__offset` < 105 AND `__offset` > 99)
  OR (`__offset` = 109);
```

Keep in mind that partitions can grow and shrink within the Kafka cluster without the consumer's knowledge. This 
partition and offset capability is good for replay of specific partitions when the consumer knows that something has 
gone wrong down stream or replay is required.  Apache Hive users may or may not understand the underlying architecture 
of Kafka therefore, filtering on the record timestamp metadata column is arguably the best filter to use since it 
requires no partition knowledge. 

The user can define a view to take of the last 15 minutes and mask what ever column as follows:

```sql
CREATE VIEW 
  last_15_minutes_of_kafka_table 
AS 
SELECT 
  `timestamp`,
  `user`, 
  `delta`, 
  `added` 
FROM 
  kafka_table 
WHERE 
  `__timestamp` >  1000 * TO_UNIX_TIMESTAMP(CURRENT_TIMESTAMP - INTERVAL '15' MINUTES);
```

Join the Kafka topic to Hive table. For instance, assume you want to join the last 15 minutes of the topic to 
a dimension table with the following example: 

```sql
CREATE TABLE 
  user_table (
  `user` STRING, 
  `first_name` STRING, 
  `age` INT, 
  `gender` STRING, 
  `comments` STRING ) 
STORED AS ORC;
```

Join the view of the last 15 minutes to `user_table`, group by the `gender` column and compute aggregates
over metrics from the fact and dimension tables.

```sql
SELECT 
  SUM(`added`) AS `added`, 
  SUM(`deleted`) AS `deleted`, 
  AVG(`delta`) AS `delta`, 
  AVG(`age`) AS `avg_age`, 
  `gender` 
FROM 
  last_15_minutes_of_kafka_table 
JOIN 
  user_table ON 
    last_15_minutes_of_kafka_table.`user` = user_table.`user`
GROUP BY 
  `gender` 
LIMIT 10;
```

In cases where you want to perform some ad-hoc analysis over the last 15 minutes of topic data,
you can join it on itself. In the following example, we show how you can perform classical 
user retention analysis over the Kafka topic.

```sql
-- Topic join over the view itself
-- The example is adapted from https://www.periscopedata.com/blog/how-to-calculate-cohort-retention-in-sql
-- Assuming l15min_wiki is a view of the last 15 minutes based on the topic's timestamp record metadata

SELECT 
  COUNT(DISTINCT `activity`.`user`) AS `active_users`, 
  COUNT(DISTINCT `future_activity`.`user`) AS `retained_users`
FROM 
  l15min_wiki AS activity
LEFT JOIN 
  l15min_wiki AS future_activity
ON
  activity.`user` = future_activity.`user`
AND 
  activity.`timestamp` = future_activity.`timestamp` - INTERVAL '5' MINUTES;

--  Topic to topic join
-- Assuming wiki_kafka_hive is the entire topic

SELECT 
  FLOOR_HOUR(activity.`timestamp`), 
  COUNT(DISTINCT activity.`user`) AS `active_users`, 
  COUNT(DISTINCT future_activity.`user`) AS retained_users
FROM 
  wiki_kafka_hive AS activity
LEFT JOIN 
  wiki_kafka_hive AS future_activity 
ON
  activity.`user` = future_activity.`user`
AND 
  activity.`timestamp` = future_activity.`timestamp` - INTERVAL '1' HOUR 
GROUP BY 
  FLOOR_HOUR(activity.`timestamp`); 
```

# Configuration

## Table Properties

| Property                            | Description                                                                                                                        | Mandatory | Default                                 |
|-------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|-----------|-----------------------------------------|
| kafka.topic                         | Kafka topic name to map the table to.                                                                                              | Yes       | null                                    |
| kafka.bootstrap.servers             | Table property indicating Kafka broker(s) connection string.                                                                       | Yes       | null                                    |
| kafka.serde.class                   | Serializer and Deserializer class implementation.                                                                                  | No        | org.apache.hadoop.hive.serde2.JsonSerDe |
| hive.kafka.poll.timeout.ms          | Parameter indicating Kafka Consumer poll timeout period in millis.  FYI this is independent from internal Kafka consumer timeouts. | No        | 5000 (5 Seconds)                        |
| hive.kafka.max.retries              | Number of retries for Kafka metadata fetch operations.                                                                             | No        | 6                                       |
| hive.kafka.metadata.poll.timeout.ms | Number of milliseconds before consumer timeout on fetching Kafka metadata.                                                         | No        | 30000 (30 Seconds)                      |
| kafka.write.semantic                | Writer semantics, allowed values (AT_LEAST_ONCE, EXACTLY_ONCE)                                                         | No        | AT_LEAST_ONCE                           |


### Setting Extra Consumer/Producer properties.
The user can inject custom Kafka consumer/producer properties via the table properties.
To do so, the user can add any key/value pair of Kafka config to the Hive table property
by prefixing the key with `kafka.consumer` for consumer configs and `kafka.producer` for producer configs.
For instance the following alter table query adds the table property `"kafka.consumer.max.poll.records" = "5000"` 
and will inject `max.poll.records=5000` to the Kafka Consumer.

```sql
ALTER TABLE 
  kafka_table 
SET TBLPROPERTIES 
  ("kafka.consumer.max.poll.records" = "5000");
```

# Kafka to Hive ETL Pipeline Example

In this example we will load topic data only once.  The goal is to read data and commit both data and 
offsets in a single Transaction 

First, create the offset table.

```sql
DROP TABLE 
  kafka_table_offsets;
  
CREATE TABLE 
  kafka_table_offsets (
  `partition_id` INT,
  `max_offset` BIGINT,
  `insert_time` TIMESTAMP);
``` 

Initialize the table

```sql
INSERT OVERWRITE TABLE 
  kafka_table_offsets 
SELECT
 `__partition`, 
 MIN(`__offset`) - 1, 
 CURRENT_TIMESTAMP 
FROM 
  wiki_kafka_hive 
GROUP BY 
  `__partition`, 
  CURRENT_TIMESTAMP;
``` 

Create the final Hive table for warehouse use,

```sql
DROP TABLE 
  orc_kafka_table;
  
CREATE TABLE 
  orc_kafka_table (
  `partition_id` INT,
  `koffset` BIGINT,
  `ktimestamp` BIGINT,
  `timestamp` TIMESTAMP,
  `page` STRING,
  `user` STRING,
  `diffurl` STRING,
  `isrobot` BOOLEAN,
  `added` INT,
  `deleted` INT,
  `delta` BIGINT) 
STORED AS ORC;
```

This is an example that inserts up to offset = 2 only.

```sql
FROM
  wiki_kafka_hive ktable 
JOIN 
  kafka_table_offsets offset_table
ON (
    ktable.`__partition` = offset_table.`partition_id`
  AND 
    ktable.`__offset` > offset_table.`max_offset` 
  AND  
    ktable.`__offset` < 3 )
    
INSERT INTO TABLE 
  orc_kafka_table 
SELECT 
  `__partition`,
  `__offset`,
  `__timestamp`,
  `timestamp`, 
  `page`, 
  `user`, 
  `diffurl`, 
  `isrobot`,
  `added`, 
  `deleted`,
  `delta`
  
INSERT OVERWRITE TABLE 
  kafka_table_offsets 
SELECT
  `__partition`,
  max(`__offset`),
  CURRENT_TIMESTAMP
GROUP BY
  `__partition`, 
  CURRENT_TIMESTAMP;
```

Double check the insert.

```sql
SELECT
  max(`koffset`) 
FROM
  orc_kafka_table 
LIMIT 10;

SELECT
  COUNT(*) AS `c`  
FROM
  orc_kafka_table
GROUP BY
  `partition_id`,
  `koffset` 
HAVING 
  `c` > 1;
```

Conduct this as data becomes available on the topic. 

```sql
FROM
  wiki_kafka_hive ktable 
JOIN 
  kafka_table_offsets offset_table
ON ( 
    ktable.`__partition` = offset_table.`partition_id`
  AND 
    ktable.`__offset` > `offset_table.max_offset`)
    
INSERT INTO TABLE 
  orc_kafka_table 
SELECT 
  `__partition`, 
  `__offset`, 
  `__timestamp`,
  `timestamp`, 
  `page`, 
  `user`, 
  `diffurl`, 
  `isrobot`, 
  `added`, 
  `deleted`, 
  `delta`
  
INSERT OVERWRITE TABLE 
  kafka_table_offsets 
SELECT
  `__partition`, 
  max(`__offset`), 
  CURRENT_TIMESTAMP 
GROUP BY 
  `__partition`, 
  CURRENT_TIMESTAMP;
```

# ETL from Hive to Kafka

## Kafka topic append with INSERT
First create the table in Hive that will be the target table. Now all the inserts will go to the topic mapped by 
this table.  Be aware that the Avro SerDe used below is regular Apache Avro (with schema) and not Confluent serialized
Avro which is popular with Kafka usage

```sql
CREATE EXTERNAL TABLE 
  moving_avg_wiki_kafka_hive ( 
    `channel` STRING, 
    `namespace` STRING,
    `page` STRING,
    `timestamp` TIMESTAMP, 
    `avg_delta` DOUBLE)
STORED BY 
  'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES(
  "kafka.topic" = "moving_avg_wiki_kafka_hive_2",
  "kafka.bootstrap.servers"="localhost:9092",
  -- STORE AS AVRO IN KAFKA
  "kafka.serde.class"="org.apache.hadoop.hive.serde2.avro.AvroSerDe");
```

Then, insert data into the table. Keep in mind that Kafka is append only thus you can not use insert overwrite. 

```sql
INSERT INTO TABLE
  moving_avg_wiki_kafka_hive 
SELECT 
  `channel`, 
  `namespace`, 
  `page`, 
  `timestamp`, 
  avg(`delta`) OVER (ORDER BY `timestamp` ASC ROWS BETWEEN 60 PRECEDING AND CURRENT ROW) AS `avg_delta`, 
  null AS `__key`, 
  null AS `__partition`, 
  -1, 
  -1 
FROM 
  l15min_wiki;
```
