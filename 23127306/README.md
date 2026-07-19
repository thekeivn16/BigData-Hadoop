# Lab 1 - Hadoop WordCount

Name: Nguyễn Phương Thảo  
Student ID: 23127306

## Environment

- Ubuntu WSL
- Java 17
- Hadoop 3.5.0
- Scala 2.11.12

## Folder structure

The final `23127306/src/WordCount` folder contains:

```text
23127306/src/WordCount
├── WordCount.scala // Scala Hadoop MapReduce source code
├── words.txt       // input dictionary file
└── results.txt     // final TSV-formatted output file for submission
```

## How to run

Start HDFS and YARN:

```bash
start-dfs.sh
start-yarn.sh
jps
```

From the `23127306` folder, go to the WordCount folder:

```bash
cd src/WordCount
```

Upload the input file to HDFS:

```bash
hdfs dfs -rm -r -f /wordcount
hdfs dfs -mkdir -p /wordcount/input
hdfs dfs -put -f words.txt /wordcount/input/
hdfs dfs -ls /wordcount/input
hdfs dfs -cat /wordcount/input/words.txt | head
```

Compile the Scala source code:

```bash
rm -rf classes scala_lib
rm -f wordcount.jar results.txt

mkdir -p classes
scalac -classpath "$(hadoop classpath)" -d classes WordCount.scala
```

Build `wordcount.jar` with Scala runtime included:

```bash
mkdir -p scala_lib

cd scala_lib
jar -xf /usr/share/scala-2.11/lib/scala-library.jar
cd ..

jar -cvf wordcount.jar -C scala_lib/ . -C classes/ .
```

Run the Hadoop MapReduce job:

```bash
hdfs dfs -rm -r -f /wordcount/output

hadoop jar wordcount.jar WordCount \
  /wordcount/input/words.txt \
  /wordcount/output
```

Check the raw output on HDFS:

```bash
hdfs dfs -ls /wordcount/output
hdfs dfs -cat /wordcount/output/part-r-00000
```

Export and reorder the result into `results.txt`:

```bash
hdfs dfs -cat /wordcount/output/part-r-00000 > results.txt

Check the final output:

```bash
cat results.txt
```