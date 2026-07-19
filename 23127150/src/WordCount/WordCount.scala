package WordCount

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}

object WordCountJob {
  def main(args: Array[String]): Unit = {
    // Require exactly two arguments: input and output paths
    if (args.length != 2) {
      println("usage: [input] [output]")
      System.exit(-1)
    }

    // Set up the job and wire in mapper, reducer, and combiner
    val job = Job.getInstance(new Configuration())
    job.setJarByClass(classOf[WordCountJob.type])
    job.setMapperClass(classOf[WordCountMapper])
    job.setReducerClass(classOf[WordCountReducer])
    job.setCombinerClass(classOf[WordCountReducer])

    // Define input/output formats and the emitted key/value types
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    // Bind the input and output paths from the arguments
    FileInputFormat.setInputPaths(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    // Submit the job and exit with its success status
    job.submit()
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}