# 23127091 - Big Data Lab 1

## Student Information

- Name: Van Thi Diem My
- Student ID: 23127091

## Environment

- OS: Windows + WSL (Ubuntu)
- Java: JDK 11
- Scala: 2.12.18
- Hadoop client dependency in project: 3.3.6

## 1. Submission Structure

Current folder structure:

```text
23127091/
|- README.md
|- docs/
|  |- 23127091_verification.txt
|  |- Report.pdf
|- src/
   |- WordCount/
      |- WordCount.scala
      |- results.txt
```

## 2. Hadoop Setup and Verification

## 2.1 Objective
Set up Hadoop (pseudo-distributed), run common HDFS commands, and generate `23127091_verification.txt` by running the provided verification JAR.

## 2.2 Required HDFS Steps
Use your own Hadoop environment:


```bash
hdfs dfs -mkdir -p /hcmus
hdfs dfs -mkdir -p /hcmus/23127091
hdfs dfs -put -f words.txt /hcmus/23127091/
hdfs dfs -chmod 744 /hcmus/23127091
hdfs dfs -chown khtn_23127091 /hcmus/23127091
```

## 2.3 Verification JAR Command
Run from your chosen runtime folder:

```bash
java -jar /path/to/hadoop-test.jar 9000 /hcmus/23127091
```

After execution, collect the generated file:

- `23127091_verification.txt`

In this repository, the verification file is stored at:

- `docs/23127091_verification.txt`

## 3. WordCount Warm-up

## 3.1 Objective
Count the number of words starting with letters:

`f, i, t, h, c, m, u, s`

## 3.2 Implementation Summary
The MapReduce program is implemented in:

- `src/WordCount/WordCount.scala`

Logic:

1. Mapper reads words and checks the first character (lowercased).
2. Only words starting with `fithcmus` are emitted.
3. A temporary numeric prefix is added to preserve final ordering.
4. Reducer removes the prefix and writes final key-value pairs in TSV.

## 3.3 Build
From workspace root (where `build.sbt` exists):

```bash
sbt clean assembly
```

Expected assembly JAR path:

```text
target/scala-2.12/wordcount-assembly-1.0.jar
```

## 3.4 Run WordCount

```bash
hdfs dfs -rm -r -f /user/vandiemmy/output
hdfs dfs -put -f words.txt /user/vandiemmy/words.txt
hadoop jar target/scala-2.12/wordcount-assembly-1.0.jar WordCount /user/vandiemmy/words.txt /user/vandiemmy/output
hdfs dfs -cat /user/vandiemmy/output/part-r-00000
```

## 3.5 Expected TSV Output
Reference output:

- `results.txt`

Expected content:

```text
f	15870
i	15220
t	25223
h	18662
c	38934
m	25194
u	23790
s	50571
```