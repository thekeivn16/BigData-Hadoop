package WordCount

import java.io.IOException
import java.util.StringTokenizer

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class WordCountMapper extends Mapper[Object, Text, Text, IntWritable] {
  // Reusable writables to avoid per-token allocation
  val one = new IntWritable(1)
  val word = new Text()

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context)
      : Unit = {

    // Split the input line into whitespace-separated tokens
    val itr = new StringTokenizer(value.toString)

    while (itr.hasMoreTokens) {
      val token = itr.nextToken().toLowerCase

      // Emit (initial, 1) only for words starting with a target letter
      if (token.length > 0) {
        val c = token.charAt(0)

        if ("fithcmus".contains(c)) {
          word.set(c.toString)
          context.write(word, one)
        }
      }
    }
  }
}