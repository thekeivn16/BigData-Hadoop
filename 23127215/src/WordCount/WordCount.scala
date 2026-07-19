// Import các thư viện cần thiết từ Hadoop framework
import org.apache.hadoop.conf.Configuration       // Cấu hình Hadoop cluster
import org.apache.hadoop.fs.Path                   // Đại diện đường dẫn file trên HDFS
import org.apache.hadoop.io.{IntWritable, Text}    // Kiểu dữ liệu serializable của Hadoop (Text ~ String, IntWritable ~ Int)
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}           // Các lớp cốt lõi của MapReduce framework
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat        // Định dạng đọc dữ liệu đầu vào từ HDFS
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat      // Định dạng ghi dữ liệu đầu ra lên HDFS

/**
 * WordCount - Chương trình đếm số từ bắt đầu bằng các ký tự cụ thể
 * sử dụng mô hình MapReduce trên Apache Hadoop.
 *
 * Luồng xử lý:
 *   1. Mapper (TokenizerMapper): Đọc từng dòng văn bản, tách thành các từ,
 *      lọc các từ bắt đầu bằng ký tự {f, i, t, h, c, m, u, s},
 *      và phát ra cặp (ký_tự_đầu, 1).
 *   2. Reducer (SumReducer): Nhận tất cả các giá trị ứng với cùng một key (ký tự đầu),
 *      cộng dồn lại để ra tổng số từ bắt đầu bằng ký tự đó.
 */
object WordCount {

  /**
   * TokenizerMapper - Lớp Mapper chịu trách nhiệm xử lý từng dòng đầu vào.
   *
   * Kiểu dữ liệu Input/Output:
   *   - Input:  (Object, Text)        → (offset byte, nội dung 1 dòng văn bản)
   *   - Output: (Text, IntWritable)   → (ký tự đầu tiên của từ, giá trị 1)
   */
  class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {

    // Giá trị hằng số 1, được tái sử dụng cho mỗi lần emit để tránh tạo object mới liên tục
    private val one = new IntWritable(1)

    // Biến Text dùng để lưu key đầu ra (ký tự đầu tiên của từ), cũng được tái sử dụng
    private val outKey = new Text()

    /**
     * Hàm map - Được gọi cho mỗi dòng trong file đầu vào.
     *
     * @param key     Offset byte của dòng hiện tại trong file (không sử dụng)
     * @param value   Nội dung text của dòng hiện tại
     * @param context Đối tượng Context dùng để ghi kết quả đầu ra (emit cặp key-value)
     */
    override def map(
        key: Object,
        value: Text,
        context: Mapper[Object, Text, Text, IntWritable]#Context
    ): Unit = {

      // Tách dòng văn bản thành mảng các từ, dùng biểu thức regex "\\s+" (1 hoặc nhiều khoảng trắng)
      val words = value.toString.split("\\s+")

      // Duyệt qua từng từ trong mảng bằng vòng lặp while (hiệu năng tốt hơn for trong Scala trên Hadoop)
      var i = 0
      while (i < words.length) {

        val w = words(i)

        // Kiểm tra từ không rỗng trước khi xử lý
        if (w.length > 0) {

          // Chuyển ký tự đầu tiên của từ về chữ thường để so sánh không phân biệt hoa/thường
          val c = Character.toLowerCase(w.charAt(0))

          // Chỉ giữ lại các từ bắt đầu bằng một trong các ký tự: f, i, t, h, c, m, u, s
          if (
            c == 'f' ||
            c == 'i' ||
            c == 't' ||
            c == 'h' ||
            c == 'c' ||
            c == 'm' ||
            c == 'u' ||
            c == 's'
          ) {
            // Gán key đầu ra là ký tự đầu tiên (dạng String)
            outKey.set(String.valueOf(c))

            // Emit cặp (ký_tự_đầu, 1) cho Reducer xử lý
            context.write(outKey, one)
          }
        }

        i += 1 // Tăng chỉ số duyệt mảng
      }
    }
  }

  /**
   * SumReducer - Lớp Reducer chịu trách nhiệm tổng hợp (aggregate) kết quả từ Mapper.
   *
   * Kiểu dữ liệu Input/Output:
   *   - Input:  (Text, Iterable[IntWritable])   → (ký tự đầu, danh sách các giá trị 1)
   *   - Output: (Text, IntWritable)             → (ký tự đầu, tổng số từ)
   */
  class SumReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

    /**
     * Hàm reduce - Được gọi cho mỗi key duy nhất (mỗi ký tự đầu tiên).
     *
     * @param key     Ký tự đầu tiên (key được nhóm từ Mapper)
     * @param values  Danh sách tất cả các giá trị IntWritable tương ứng với key này
     * @param context Đối tượng Context dùng để ghi kết quả đầu ra cuối cùng
     */
    override def reduce(
        key: Text,
        values: java.lang.Iterable[IntWritable],
        context: Reducer[Text, IntWritable, Text, IntWritable]#Context
    ): Unit = {

      // Biến tích lũy để cộng dồn tổng số từ
      var sum = 0

      // Lấy iterator để duyệt qua danh sách các giá trị
      val iter = values.iterator()

      // Duyệt qua từng giá trị và cộng dồn vào biến sum
      while (iter.hasNext) {
        sum += iter.next().get() // .get() chuyển IntWritable → Int
      }

      // Ghi kết quả cuối cùng: (ký_tự_đầu, tổng_số_từ) ra output
      context.write(key, new IntWritable(sum))
    }
  }

  /**
   * Hàm main - Điểm bắt đầu thực thi chương trình.
   * Cấu hình và khởi chạy MapReduce job trên Hadoop cluster.
   *
   * @param args Tham số dòng lệnh: args(0) = đường dẫn input, args(1) = đường dẫn output
   */
  def main(args: Array[String]): Unit = {

    // Kiểm tra số lượng tham số đầu vào, yêu cầu đúng 2 tham số (input path và output path)
    if (args.length != 2) {
      System.err.println("Usage: WordCount <input> <output>")
      System.exit(1) // Thoát chương trình với mã lỗi 1 nếu thiếu tham số
    }

    // Tạo đối tượng Configuration mặc định của Hadoop (đọc các file cấu hình core-site.xml, hdfs-site.xml, ...)
    val conf = new Configuration()

    // Tạo một MapReduce Job mới với tên "WordCount"
    val job = Job.getInstance(conf, "WordCount")

    // Chỉ định file JAR chứa class chính để Hadoop phân phối tới các node trong cluster
    job.setJarByClass(WordCount.getClass)

    // Đăng ký lớp Mapper và Reducer cho job
    job.setMapperClass(classOf[TokenizerMapper])   // Mapper: TokenizerMapper
    job.setReducerClass(classOf[SumReducer])       // Reducer: SumReducer

    // Thiết lập kiểu dữ liệu output của job (key: Text, value: IntWritable)
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    // Thiết lập đường dẫn input và output trên HDFS từ tham số dòng lệnh
    FileInputFormat.addInputPath(job, new Path(args(0)))    // Đường dẫn chứa dữ liệu đầu vào
    FileOutputFormat.setOutputPath(job, new Path(args(1)))  // Đường dẫn ghi kết quả đầu ra

    // Chạy job và chờ hoàn thành; thoát với mã 0 (thành công) hoặc 1 (thất bại)
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}