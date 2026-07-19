package WordCount

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

class WordCountReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  // Reusable writable for the emitted count
  val result = new IntWritable()

  override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
                      context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    // Sum all counts for this key
    var sum = 0
    val iter = values.iterator()

    while (iter.hasNext) {
      sum += iter.next().get()
    }

    // Emit the key with its total count
    result.set(sum)
    context.write(key, result)
  }
}