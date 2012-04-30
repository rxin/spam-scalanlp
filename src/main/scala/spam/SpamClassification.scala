package spam

import scalala.collection.sparse.SparseArray
import scalala.tensor.sparse.SparseVectorRow
import scalanlp.data.Example
import scalanlp.util.logging.ConsoleLogging
import scalanlp.classify._

import scala.io.Source


object SpamClassification {

  def main(args: Array[String]) {
    svm(args(0), args(1).toInt) 
  }

  def naiveBayes(path: String) = {
    val (dict, trainingData) = spam.SpamClassification.buildFeatureMatrix(
      path + "/feature-matrix.txt", path + "/feature-list.txt")

    val data = trainingData.map { d =>
      Example(d.label, scalala.tensor.Counter(d.features.data.toMap), d.id)
    }
    val classifier = new scalanlp.classify.NaiveBayes(data, 0.1, 0.01)

    val rand = new scala.util.Random

    (0 until 10).foreach { i =>
      var correct = 0
      var incorrect = 0
      val samples = (0 until 2000).map(j => data(rand.nextInt(data.size)))

      samples.foreach { d =>
        val guessed = classifier.classify(d.features)
        //println(guessed, d.label)
        if (guessed == d.label) {
          correct += 1
        } else {
          incorrect += 1
        }
      }
      println("%d correct, %d incorrect, %d".format(
        correct, incorrect, correct * 100 / (correct + incorrect)))
    }

    classifier
  }

  def logistics(path: String) = {
    val (dict, trainingData) = spam.SpamClassification.buildFeatureMatrix(
      path + "/feature-matrix.txt", path + "/feature-list.txt")

    val trainer = new LogisticClassifier.Trainer[Int, SparseVectorRow[Double]]
    val classifier = trainer.train(trainingData)

    // print out highest weighted terms
    println
    println("highest weight features for ham")
    classifier.featureWeights(0).toList.zipWithIndex.sortBy(x => Math.abs(x._1)).reverse
      .take(20).foreach { f => println(f._2, dict(f._2), f._1) }

    println
    println("highest weight features for spam")
    classifier.featureWeights(1).toList.zipWithIndex.sortBy(x => Math.abs(x._1)).reverse
      .take(20).foreach { f => println(f._2, dict(f._2), f._1) }

    val rand = new scala.util.Random

    (0 until 10).foreach { i =>
      var correct = 0
      var incorrect = 0
      val samples = (0 until 2000).map(j => trainingData(rand.nextInt(trainingData.size)))

      samples.foreach { d =>
        val guessed = classifier.classify(d.features)
        //println(guessed, d.label)
        if (guessed == d.label) {
          correct += 1
        } else {
          incorrect += 1
        }
      }

      println("%d correct, %d incorrect, %d".format(
        correct, incorrect, correct * 100 / (correct + incorrect)))

    }

    classifier
  }

  def svm(path: String, numIter: Int) = {
    val (dict, trainingData) = spam.SpamClassification.buildFeatureMatrix(
      path + "/feature-matrix.txt", path + "/feature-list.txt")

    val trainer = new SVM.Pegasos[Int, SparseVectorRow[Double]](
      numIter, batchSize=1000) with ConsoleLogging

    val classifier = trainer.train(trainingData)

    // print out highest weighted terms
    println
    println("highest weight features for ham")
    classifier.featureWeights(0).toList.zipWithIndex.sortBy(x => Math.abs(x._1)).reverse
      .take(20).foreach { f => println(f._2, dict(f._2), f._1) }

    println
    println("highest weight features for spam")
    classifier.featureWeights(1).toList.zipWithIndex.sortBy(x => Math.abs(x._1)).reverse
      .take(20).foreach { f => println(f._2, dict(f._2), f._1) }

    val rand = new scala.util.Random

    (0 until 10).foreach { i =>
      var correct = 0
      var incorrect = 0
      val samples = (0 until 2000).map(j => trainingData(rand.nextInt(trainingData.size)))

      samples.foreach { d =>
        val guessed = classifier.classify(d.features)
        //println(guessed, d.label)
        if (guessed == d.label) {
          correct += 1
        } else {
          incorrect += 1
        }
      }

      println("%d correct, %d incorrect, %d".format(
        correct, incorrect, correct * 100 / (correct + incorrect)))

    }

    classifier
  }

  def buildFeatureMatrix(docpath: String, dictPath: String) = {

    // read the dictionary
    val dict = collection.immutable.Map[Int, String]() ++
      Source.fromFile(dictPath).getLines.map { line =>
        val fields = line.split("\\t")
        (fields(0).toInt, fields(1))
      }
    println("dictionary size: " + dict.size)

    // read the document
    val chunkSize = 128 * 1024
    val iterator = Source.fromFile(docpath).getLines.grouped(chunkSize)
    val trainingData = iterator.flatMap { lines => 
      lines.par.map { line =>
      //lines.map { line =>

        val fields = line.split(", ")
        val id = fields(0)
        val label = fields(1).toInt

        val arr = new SparseArray[Double](dict.size)

        var i = 2
        assert(fields.length % 2 == 0)
        while (i < fields.length) {
          arr.update(fields(i).toInt, fields(i+1).toDouble)
          i += 2
        }

        val features = new SparseVectorRow[Double](arr)
        Example(label, features, id)
      }
    }.filter(_.features.nonzeroSize > 0).toSeq

    println("training data size: " + trainingData.size)

    (dict, trainingData)
  }

}

