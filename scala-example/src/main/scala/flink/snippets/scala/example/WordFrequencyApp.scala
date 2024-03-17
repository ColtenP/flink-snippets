package flink.snippets.scala.example

import flink.snippets.scala.example.model.WordFrequency
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.flink.util.Collector

import scala.jdk.CollectionConverters.IteratorHasAsScala

object WordFrequencyApp {
  // Used to provide the runtime, if you were running this on an actual flink cluster,
  // then this would not be needed, but we're doing this for testing sakes!
  private val flinkCluster: MiniClusterWithClientResource =
    new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
        .setNumberSlotsPerTaskManager(1)
        .setNumberTaskManagers(1)
        .build()
    )

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // Used for testing purposes (We want our Flink job to quit after we've seen our last input record)
    env.setRuntimeMode(RuntimeExecutionMode.BATCH)

    // Collect a few sentences into a Flink Source so we can break them down to find word frequency
    val sentences = env.fromElements(
      "The cat quietly prowled through the dark alley.",
      "Raindrops softly tapped against the windowpane.",
      "Birds chirped melodiously in the early morning light.",
      "The river gently flowed beneath the old stone bridge.",
      "Leaves rustled softly in the autumn breeze."
    )

    val wordFrequency = sentences
      // Take our sentences, lowercase them, split them apart by spaces,
      // remove any non-letter characters (remove the period),
      // then collect them out as a WordFrequency with a count of 1
      .flatMap(
        (sentence: String, out: Collector[WordFrequency]) => {
          sentence
            .toLowerCase()
            .split(' ')
            .map(_.replaceAll("[^a-zA-Z]", ""))
            .map(word => WordFrequency(word, count = 1))
            .foreach(out.collect)
        },
        // We need to supply the TypeInformation of the class we're collec
        TypeInformation.of(classOf[WordFrequency])
      )
      // Key the stream by the word of the WordFrequency case class
      .keyBy((frequency: WordFrequency) => frequency.word)
      // Window the stream into 10 second windows using TumblingProcessingTime
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      // Reduce each WordFrequency down by summing the count attribute with an accumulator
      .reduce((accumulator: WordFrequency, frequency: WordFrequency) =>
        accumulator.copy(count = accumulator.count + frequency.count)
      )

    // For every word frequency we see, print out the results to the console
    wordFrequency
      // Execute the code and collect it to an iterator, then convert it as a scala seq
      .executeAndCollect().asScala.toSeq
      // We only want words that were seen more than once, then sort descending by count
      .filter(_.count > 1)
      .sortBy(- _.count)
      .foreach { frequency =>
        Console.println(s"""Word "${frequency.word}" was seen ${frequency.count} times.""")
      }
  }
}