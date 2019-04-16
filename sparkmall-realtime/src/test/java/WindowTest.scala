import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}

object WindowTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WindowTest")

    val ssc = new StreamingContext(conf,Durations.seconds(1))

    val line: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)
    val words: DStream[String] = line.flatMap(_.split(" "))

    val windowsWords: DStream[String] = words.window(Seconds(3),Seconds(1))

    windowsWords.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
