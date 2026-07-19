# WordCount trên Hadoop MapReduce (Scala + sbt assembly)

Đây là hướng dẫn cách build và chạy 3 file Scala trong thư mục `src/WordCount/` bằng một
project `sbt` có dùng plugin `sbt-assembly`. Toàn bộ quá trình cài đặt Hadoop (Pseudo mode),
cài `sbt` cũng như lí do vì sao cần từng bước, em trình bày chi tiết trong báo cáo
[docs/Report.pdf](docs/Report.pdf).

## Cấu trúc code

Trong repo, 3 file code nằm ở:

```
src/WordCount/
├── WordCount.scala   # driver: cấu hình và submit job MapReduce
├── Mapper.scala      # phần Map: emit (ký tự đầu, 1) cho các từ bắt đầu bằng f/i/t/h/c/m/u/s
└── Reducer.scala     # phần Reduce: cộng dồn để ra số lần xuất hiện
```

Phần giải thích chi tiết từng file có ở mục *"Viết code Scala cho WordCount"* trong
[docs/Report.pdf](docs/Report.pdf).

## Yêu cầu cần có trước

- JDK 17 (khớp với Hadoop 3.5.0).
- Hadoop 3.5.0 đã cài và HDFS đang chạy ở Pseudo mode (xem cách cài trong
  [docs/Report.pdf](docs/Report.pdf)).
- `sbt` đã cài trên máy (mục *"Cài đặt `sbt` trên Ubuntu"* trong
  [docs/Report.pdf](docs/Report.pdf)).

## 1. Tạo project sbt

Đứng ở thư mục `home` (`~`) và tạo một project Scala mới tên `wordcount` bằng `sbt`:

```bash
sbt new scala/scala3.g8
```

Lệnh này tạo ra project theo template `scala3.g8` với cấu trúc chuẩn (`build.sbt`,
`project/`, `src/main/scala/`, `src/test/scala/`, `README.md`).

## 2. Đặt 3 file code vào đúng chỗ

Code Scala chính phải nằm trong thư mục `src/main/scala/` của project. Vì cả 3 file đều khai
báo `package WordCount`, em để chúng trong một thư mục con `src/main/scala/WordCount/` cho
gọn. Copy 3 file từ repo này sang project vừa tạo và xóa file `Main.scala` mà template tạo
sẵn:

```bash
mkdir -p ~/wordcount/src/main/scala/WordCount
cp src/WordCount/WordCount.scala \
   src/WordCount/Mapper.scala \
   src/WordCount/Reducer.scala \
   ~/wordcount/src/main/scala/WordCount/
rm -f ~/wordcount/src/main/scala/Main.scala
```

Sau bước này, cây thư mục code trong project sẽ là:

```
~/wordcount/
└── src/main/scala/WordCount/
    ├── WordCount.scala
    ├── Mapper.scala
    └── Reducer.scala
```

## 3. Cấu hình `build.sbt`

Mở file `~/wordcount/build.sbt` và sửa lại thành như sau (thêm dependency `hadoop-client` và
chiến lược merge cho `sbt-assembly`):

```scala
val scala3Version = "3.8.4"

lazy val root = project
  .in(file("."))
  .settings(
    name := "wordcount",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "1.3.2" % Test,
      "org.apache.hadoop" % "hadoop-client" % "3.5.0"
    ),

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )
```

Trong đó:

- Dòng `"org.apache.hadoop" % "hadoop-client" % "3.5.0"` kéo về các API của Hadoop (`Job`,
  `Mapper`, `Reducer`, `Text`, `IntWritable`...) để code compile được. Chọn đúng phiên bản
  `3.5.0` để khớp với Hadoop đang cài trên máy.
- Khối `assemblyMergeStrategy` quy định cách xử lý các file bị trùng đường dẫn khi gộp tất cả
  dependencies vào một fat JAR: bỏ (`discard`) các file trong `META-INF` và giữ file gặp đầu
  tiên (`first`) với phần còn lại. Không khai báo khối này thì `sbt-assembly` sẽ báo lỗi
  conflict và dừng lại.

## 4. Thêm plugin `sbt-assembly`

Tạo file `~/wordcount/project/plugins.sbt` với nội dung:

```scala
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.3.1")
```

Lí do cần dùng `sbt-assembly`: file JAR do `sbt` build ra theo cách thường chỉ chứa code của
mình, không đóng gói kèm Scala library và Hadoop client, nên khi Hadoop chạy sẽ bị
`NoClassDefFoundError`. Plugin này gộp toàn bộ dependencies vào một fat JAR duy nhất để khắc
phục.

## 5. Build fat JAR

Đứng trong thư mục project (`~/wordcount`) và chạy:

```bash
sbt clean assembly
```

File JAR sẽ được tạo ở:

```
~/wordcount/target/scala-3.8.4/wordcount-assembly-0.1.0-SNAPSHOT.jar
```

## 6. Chuẩn bị dữ liệu input trên HDFS

Các lệnh `bin/hdfs` và `bin/hadoop` chạy trong thư mục cài Hadoop (ví dụ `~/hadoop-3.5.0`).
Tạo thư mục `input` trên HDFS và đẩy file text cần đếm vào đó:

```bash
bin/hdfs dfs -mkdir /input
bin/hdfs dfs -put ~/Downloads/words.txt /input
```

## 7. Chạy job WordCount với fat JAR

Tham số lần lượt là thư mục input và thư mục output trên HDFS (thư mục output phải **chưa tồn
tại**, nếu đã có Hadoop sẽ báo lỗi):

```bash
bin/hadoop jar ~/wordcount/target/scala-3.8.4/wordcount-assembly-0.1.0-SNAPSHOT.jar /input /output
```

## 8. Lấy kết quả

Kết quả nằm trong thư mục `/output` trên HDFS, gồm file `_SUCCESS` và `part-r-00000`. Export
file kết quả về máy local dưới dạng txt (tsv-formatted) rồi in ra xem:

```bash
bin/hdfs dfs -get /output/part-r-00000 ~/Downloads/results.txt
cat ~/Downloads/results.txt
```

Mỗi dòng trong `results.txt` có dạng `<ký tự><tab><số lần xuất hiện>`, tương ứng số từ bắt
đầu bằng mỗi ký tự trong tập `f i t h c m u s` (các chữ cái của `FIT HCMUS`).
