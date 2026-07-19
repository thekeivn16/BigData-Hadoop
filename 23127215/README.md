# WordCount - Hadoop MapReduce (Scala)

Chương trình MapReduce đếm số lượng từ bắt đầu bằng các ký tự **f, i, t, h, c, m, u, s** trong tệp dữ liệu đầu vào.

## Yêu cầu hệ thống

- **Ubuntu** (hoặc hệ điều hành Linux tương đương)
- **Java JDK** (17)
- **Scala** compiler (`scalac`)
- **Hadoop** (đã cài đặt và cấu hình, HDFS đang chạy)

## Cấu trúc project

```
23127215/
├── src/
│   └── WordCount/
│       └── WordCount.scala    # Mã nguồn chương trình MapReduce
├── docs/
└── README.md
```

## Hướng dẫn chạy chương trình

### Bước 1: Khởi động Hadoop

Đảm bảo các dịch vụ Hadoop (HDFS & YARN) đã được khởi động:

```bash
start-dfs.sh
start-yarn.sh
```

Kiểm tra các tiến trình đang chạy:

```bash
jps
```

### Bước 2: Chuẩn bị dữ liệu đầu vào trên HDFS

Tải tệp `words.txt` từ Google Drive về Ubuntu:

```bash
gdown https://drive.google.com/uc?id=<FILE_ID>
```

Tạo thư mục trên HDFS (nếu thư mục đã tồn tại sẽ không báo lỗi):

```bash
hdfs dfs -mkdir -p /wordcount
```

Đưa tệp lên HDFS:

```bash
hdfs dfs -put /home/<username>/words.txt /wordcount/
```

Kiểm tra tệp đã được tải lên thành công:

```bash
hdfs dfs -ls /wordcount
```

### Bước 3: Sao chép mã nguồn vào thư mục làm việc

### Bước 4: Biên dịch và đóng gói chương trình

Biên dịch mã nguồn Scala:

```bash
scalac -classpath ".:$(hadoop classpath)" WordCount.scala
```

> **Giải thích:** `scalac` biên dịch mã nguồn Scala thành các tệp `.class`. Flag `-classpath ".:$(hadoop classpath)"` thêm toàn bộ thư viện Hadoop vào classpath để chương trình có thể sử dụng API MapReduce.

Đóng gói thành JAR:

```bash
jar -cvf wordcount.jar *.class
```

Kiểm tra nội dung JAR:

```bash
jar tf wordcount.jar
```

Kết quả sẽ liệt kê các class:
```
META-INF/MANIFEST.MF
WordCount$.class
WordCount$SumReducer.class
WordCount$TokenizerMapper.class
WordCount.class
```

### Bước 5: Thực thi chương trình WordCount

> **Lưu ý:** Nếu thư mục `/output` đã tồn tại trên HDFS, cần xóa trước khi chạy:
> ```bash
> hdfs dfs -rm -r /output
> ```

Chạy chương trình MapReduce trên Hadoop:

```bash
hadoop jar wordcount.jar WordCount /wordcount/words.txt /output
```

Trong đó:
- `wordcount.jar` — tệp JAR chứa chương trình đã biên dịch
- `WordCount` — lớp chính (main class) của chương trình
- `/wordcount/words.txt` — tệp dữ liệu đầu vào trên HDFS
- `/output` — thư mục đầu ra trên HDFS, nơi Hadoop lưu kết quả xử lý

Kiểm tra kết quả:

```bash
hdfs dfs -cat /output/part-r-00000
```

### Bước 6: Xuất kết quả từ HDFS về hệ thống cục bộ

```bash
hdfs dfs -get /output/part-r-00000 results_raw.txt
```

> **Giải thích:**
> - `/output/part-r-00000` là tệp kết quả do Reduce tạo ra trên HDFS.
> - `results_raw.txt` là tên tệp lưu trên hệ thống cục bộ sau khi tải về.

### Bước 7: Sắp xếp kết quả theo thứ tự yêu cầu

Sắp xếp lại kết quả theo đúng thứ tự f, i, t, h, c, m, u, s:

```bash
(grep '^f' results_raw.txt; \
grep '^i' results_raw.txt; \
grep '^t' results_raw.txt; \
grep '^h' results_raw.txt; \
grep '^c' results_raw.txt; \
grep '^m' results_raw.txt; \
grep '^u' results_raw.txt; \
grep '^s' results_raw.txt) > results.txt
```

Xem kết quả cuối cùng:

```bash
cat results.txt
```

## Mô tả chương trình

Chương trình gồm 2 thành phần chính:

| Thành phần | Mô tả |
|---|---|
| **TokenizerMapper** | Đọc từng dòng văn bản, tách thành các từ. Nếu từ bắt đầu bằng một trong các ký tự `f, i, t, h, c, m, u, s` (không phân biệt hoa thường). |
| **SumReducer** | Cộng tổng lại để ra số lượng từ bắt đầu bằng ký tự đó. |
