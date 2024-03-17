package flink.snippets.scala.example.model

// Our case class that will be used to reduce down our words into their counts
case class WordFrequency(word: String, count: Int)
