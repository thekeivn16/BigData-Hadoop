import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text, WritableComparable, WritableComparator}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

/**
 * Mapper: Đọc mỗi dòng, tách từ, phát ra (chữ_cái_đầu, 1)
 * nếu chữ cái đầu thuộc tập {f, i, t, h, c, m, u, s}
 */
class WordCountMapper extends Mapper[LongWritable, Text, Text, IntWritable] {

  private val one = new IntWritable(1)
  private val outputKey = new Text()
  private val targetLetters = Set("f", "i", "t", "h", "c", "m", "u", "s")

  override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, Text, IntWritable]#Context
  ): Unit = {
    // Tách dòng thành các từ bằng whitespace
    val words = value.toString.split("\\s+")

    for (word <- words) {
      // Chuyển thành chữ thường và loại bỏ khoảng trắng thừa
      val cleanedWord = word.trim.toLowerCase

      if (cleanedWord.nonEmpty) {
        // Lấy ký tự đầu tiên của từ
        val firstChar = cleanedWord.substring(0, 1)

        // Nếu ký tự đầu tiên thuộc tập mục tiêu, phát ra cặp (firstChar, 1)
        if (targetLetters.contains(firstChar)) {
          outputKey.set(firstChar)
          context.write(outputKey, one)
        }
      }
    }
  }
}

/**
 * Reducer: Nhận tất cả các giá trị (1) cùng một chữ cái đầu,
 * cộng lại để ra tổng số từ bắt đầu bằng chữ cái đó
 */
class WordCountReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

  private val result = new IntWritable()

  override def reduce(
      key: Text,
      values: java.lang.Iterable[IntWritable],
      context: Reducer[Text, IntWritable, Text, IntWritable]#Context
  ): Unit = {
    var sum = 0
    val iterator = values.iterator()

    // Duyệt qua tất cả các giá trị và cộng lại
    while (iterator.hasNext) {
      sum += iterator.next().get()
    }

    result.set(sum)
    context.write(key, result)
  }
}

/**
 * Comparator: Sắp xếp kết quả theo thứ tự tùy chỉnh: f i t h c m u s
 */
class LetterComparator extends WritableComparator(classOf[Text], true) {

  // Ánh xạ: chữ cái -> vị trí sắp xếp
  private val order = Map(
    "f" -> 0,
    "i" -> 1,
    "t" -> 2,
    "h" -> 3,
    "c" -> 4,
    "m" -> 5,
    "u" -> 6,
    "s" -> 7
  )

  /**
   * So sánh hai khóa để sắp xếp
   * Trả về: âm nếu a < b, dương nếu a > b, 0 nếu a = b
   */
  override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
    val key1 = a.asInstanceOf[Text].toString
    val key2 = b.asInstanceOf[Text].toString

    // Lấy vị trí sắp xếp, nếu không có thì gán Int.MaxValue (cuối cùng)
    val pos1 = order.getOrElse(key1, Int.MaxValue)
    val pos2 = order.getOrElse(key2, Int.MaxValue)

    Integer.compare(pos1, pos2)
  }
}

object WordCount {

  def main(args: Array[String]): Unit = {
    // Kiểm tra số lượng tham số đầu vào
    if (args.length != 2) {
      System.err.println("Usage: WordCount <input_path> <output_path>")
      System.exit(1)
    }

    // Khởi tạo cấu hình và job
    val conf = new Configuration()
    val job = Job.getInstance(conf, "WordCount")
    job.setJarByClass(this.getClass)

    // Thiết lập Mapper, Reducer và Comparator
    job.setMapperClass(classOf[WordCountMapper])
    job.setReducerClass(classOf[WordCountReducer])
    job.setSortComparatorClass(classOf[LetterComparator])

    // Sử dụng 1 Reducer để đảm bảo thứ tự kết quả
    job.setNumReduceTasks(1)

    // Thiết lập kiểu dữ liệu output
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    // Thiết lập đường dẫn input/output
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    // Chạy job
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}