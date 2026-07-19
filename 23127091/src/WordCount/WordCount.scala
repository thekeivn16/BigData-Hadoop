import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import scala.collection.JavaConverters._

/**
 * Mapper lọc các từ theo chữ cái đầu và phát ra cặp (chữ cái, 1).
 *
 * Ý tưởng:
 * - Chỉ giữ lại các từ bắt đầu bằng một trong các chữ: f, i, t, h, c, m, u, s.
 * - Để đảm bảo thứ tự kết quả đúng theo chuỗi "fithcmus",
 *   key trung gian sẽ được gắn tiền tố số thứ tự (ví dụ: "0f", "1i", ...).
 */
class FilteredMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  // Tập chữ cái đầu cần thống kê.
  val targetLetters = Set('f', 'i', 't', 'h', 'c', 'm', 'u', 's')

  // Bảng ánh xạ chữ cái -> vị trí ưu tiên trong thứ tự "fithcmus".
  val letterOrder: Map[Char, Int] =
    targetLetters.toSeq.sortBy(c => "fithcmus".indexOf(c)).zipWithIndex.toMap

  // Giá trị đếm cố định cho mỗi lần xuất hiện.
  val one = new IntWritable(1)

  // Biến key tái sử dụng để giảm tạo object mới trong vòng lặp map.
  val keyOut = new Text()

  override def map(key: LongWritable, value: Text,
      context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    // Tách dòng thành các từ theo khoảng trắng (space, tab, xuống dòng...).
    val words = value.toString.split("\\s+")
    for (w <- words if w.nonEmpty) {
      val firstChar = w.charAt(0).toLower
      if (targetLetters.contains(firstChar)) {
        // Tạo key trung gian có tiền tố số để Hadoop sắp xếp đúng thứ tự mong muốn.
        val order = letterOrder(firstChar)
        keyOut.set(s"$order$firstChar")
        context.write(keyOut, one)
      }
    }
  }
}

/**
 * Combiner cộng cục bộ trên mỗi Mapper để giảm dữ liệu truyền qua mạng.
 *
 * Đầu vào/đầu ra đều là (Text, IntWritable), nên an toàn khi dùng phép cộng.
 */
class IntSumCombiner extends Reducer[Text, IntWritable, Text, IntWritable] {
  override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
      context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    val sum = values.asScala.map(_.get()).sum
    context.write(key, new IntWritable(sum))
  }
}

/**
 * Reducer tổng hợp kết quả cuối cùng.
 *
 * Key nhận vào có dạng "<thứ_tự><chữ_cái>" (ví dụ: "0f").
 * Khi xuất kết quả, reducer bỏ tiền tố số để chỉ còn chữ cái thực tế.
 */
class IntSumReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  val keyOut = new Text()

  override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
      context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    val sum = values.asScala.map(_.get()).sum
    // Bỏ ký tự đầu tiên (số thứ tự) trước khi ghi ra output cuối.
    keyOut.set(key.toString.substring(1))
    context.write(keyOut, new IntWritable(sum))
  }
}

/**
 * Điểm vào chương trình Hadoop MapReduce.
 *
 * Tham số dòng lệnh:
 * - args(0): đường dẫn input trên HDFS/local
 * - args(1): đường dẫn output (phải chưa tồn tại)
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val job = Job.getInstance(conf, "WordCount")
    job.setJarByClass(this.getClass)

    // Khai báo 3 thành phần chính của pipeline MapReduce.
    job.setMapperClass(classOf[FilteredMapper])
    job.setCombinerClass(classOf[IntSumCombiner])
    job.setReducerClass(classOf[IntSumReducer])

    // Kiểu dữ liệu output cuối cùng của job.
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    // Cấu hình input/output path cho job.
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    // Trả mã thoát 0 nếu thành công, ngược lại trả 1.
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}