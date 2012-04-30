package spam

import spark._
import SparkContext._

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapred.JobConf

import org.apache.lucene.analysis._
import org.apache.lucene.analysis.standard.ClassicTokenizer
import org.apache.lucene.analysis.tokenattributes._
import org.apache.lucene.util.Version.LUCENE_35

import org.yagnus.yadoop.IntArrayWritable
import org.yagnus.yadoop.Yadoop.intSeq2IntArrayWritable

import java.io.{File, FileWriter, StringReader}
import scala.collection.immutable.HashMap
import scala.collection.mutable.MutableList
import scala.util.matching.Regex


object FeatureGeneration {

  def main(args: Array[String]) {

    val spamInput = args(1)
    val hamInput = args(2)
    val baseOutputPath = args(3)

    (new File(baseOutputPath)).mkdirs()
    //(new File(baseOutputPath + "/stem")).mkdirs()
    //(new File(baseOutputPath + "/stop")).mkdirs()
    //(new File(baseOutputPath + "/stemStop")).mkdirs()

    val sc = new SparkContext(args(0), "Spam-ScalaNLP")
    val hamrdd = sc.textFile(hamInput).map((0, _))
    val spamrdd = sc.textFile(spamInput).map((1, _))
    val rdd = hamrdd.union(spamrdd)

    runPipeline(rdd, FeatureGeneration.defaultTok, baseOutputPath)
    //runPipeline(rdd, FeatureGeneration.stemTok, baseOutputPath + "/stem")
    //runPipeline(rdd, FeatureGeneration.stopTok, baseOutputPath + "/stop")
    //runPipeline(rdd, FeatureGeneration.stemStopTok, baseOutputPath + "/stemStop")
  }

  def runPipeline(
    rdd: RDD[Pair[Int, String]],
    tok: (String => TokenStream),
    outpath: String) {
    // Do the word count and generate the dictionary.
    val (encodingDict, dfDict) = produceFeatureDictionary(rdd, tok, outpath)
    generateFeatures(rdd, encodingDict, tok, outpath)
  }

  def generateFeatures(
    rdd: RDD[Pair[Int, String]],
    encodingDict: Map[String, Int],
    tok: (String => TokenStream),
    outpath: String) {

    // Tokenized doc.
    val tokenizedDocs: RDD[(Int, Seq[Int])] = rdd.map { case(label, line) =>

      val fields = line.split("\\t")
      val docId = fields(1).toInt
      val text = fields(25)

      val tokens = tokenizeText(text, tok).filter(encodingDict.contains(_)).map(encodingDict(_))
      val tokensBagOfWords: Seq[Pair[Int, Int]] = tokens.get.groupBy(x => x).mapValues(_.length)
        .toSeq.sortBy(_._1)

      val nonTextFeatures: Seq[Pair[Int, Int]] = Features.features.zipWithIndex.map { case(f, i) =>
        (i, f._2(fields))
      }

      val features: Seq[Int] =
        nonTextFeatures.flatMap(x => x.productIterator).asInstanceOf[Seq[Int]] ++ 
        tokensBagOfWords.flatMap(x => x.productIterator).asInstanceOf[Seq[Int]]

      (docId, label +: features)
    }

    //tokenizedDocs.saveAsSequenceFile(outpath + "/feature-matrix-sequence")
    tokenizedDocs.map { doc =>
      doc._1 + doc._2.map(_.toString).foldLeft("")(_ + ", " + _)
    }.saveAsTextFile(outpath + "/feature-matrix-text")
  }

  def produceFeatureDictionary(
    rdd: RDD[Pair[Int, String]],
    tok: (String => TokenStream),
    outpath: String)
  : (Map[String, Int], Map[Int, Int]) = {

    var dfSorted: Seq[(String, Int)] = rdd.flatMap { line =>
      tokenizeText(line._2.split("\\t")(25), tok).distinct
    }.map((_, 1))
    .reduceByKey(_ + _)
    .filter{case(w, c) => c > 5}
    .collect()
    .sortBy(_._2)
    .reverse
    .toSeq

    dfSorted = Features.features.map(f => (f._1, 9999999)) ++ dfSorted
    writeArrayOfPairToDisk(dfSorted, outpath + "/feature-list.txt", true)

    // encodingDict maps a word to its integer id.
    val encodingDict = dfSorted.zipWithIndex.map { case(wc, i) => (wc._1, i) }.toMap
    // dfDict maps an integer id to the document frequency of the word.
    val dfDict = dfSorted.zipWithIndex.map { case(wc, i) => (i, wc._2) }.toMap
    (encodingDict, dfDict)
  }

  /**
   * Given some text in string, tokenize it and return a sequence of tokens.
   */
  def tokenizeText(text: String, tok: (String => TokenStream)): Seq[String] = {
    val tokenStream = tok(text)
    val termAtt: TermAttribute = tokenStream.addAttribute(classOf[TermAttribute])
    val typeAtt: TypeAttribute = tokenStream.addAttribute(classOf[TypeAttribute])
    val tokens = new MutableList[String]()
    while (tokenStream.incrementToken()) { tokens += termAtt.term() }
    tokenStream.end()
    tokenStream.close()
    tokens
  }
 
  def defaultTok(text: String): TokenStream = {
    new LowerCaseFilter(LUCENE_35, new ClassicTokenizer(LUCENE_35, new StringReader(text)))
  }

  def stemTok(text: String): TokenStream = {
    new PorterStemFilter(defaultTok(text))
  }

  def stopTok(text: String): TokenStream = {
    new StopFilter(LUCENE_35, defaultTok(text), StopAnalyzer.ENGLISH_STOP_WORDS_SET)
  }

  def stemStopTok(text: String): TokenStream = {
    new StopFilter(LUCENE_35, stemTok(text), StopAnalyzer.ENGLISH_STOP_WORDS_SET)
  }

  def writeArrayOfPairToDisk[K, V](
    arr: Seq[(K, V)], path: String, addLineNumber: Boolean = false) {
    val out = new FileWriter(path)
    arr.zipWithIndex.foreach { case(pair, lineNumber) => {
      if (addLineNumber) {
        out.write(lineNumber + "\t")
      }
      out.write(pair._1 + "\t" + pair._2 + "\n")
    }}
    out.close()
  }
   
}


object Features {

  // List of features to extract. The first field is the feature's ID, and the
  // second field is a function taking an array of fields and emitting an
  // integer value for the feature.
  val features = Seq[Pair[String, Array[String] => Int]](
    ("F-sex", sex),
    ("F-photo_exists", photo_exists),
    ("F-age", age),
    ("F-recip_age", recip_age),
    //("F-account_lifetime", account_lifetime),
    ("F-friends", friends),
    ("F-prof_txt_fields_completed", prof_txt_fields_completed),
    ("F-month_of_birth", month_of_birth),
    ("F-country_match", country_match),
    ("F-spam_country", is_in_spam_country)
  )

  def sex(fields: Array[String]): Int = if (fields(11) == "male") 1 else 0

  def photo_exists(fields: Array[String]): Int = fields(12).toInt

  def age(fields: Array[String]): Int = fields(6).toInt / 5

  def recip_age(fields: Array[String]): Int = {
    if (fields(20)(0) == '\\') 0 else fields(20).toInt / 5
  }

  def account_lifetime(fields: Array[String]): Int = fields(17).toInt / (24 * 60 * 60)

  def friends(fields: Array[String]): Int = {
    if (fields(18)(0) == '\\') 0 else fields(18).toInt / 5
  }

  def prof_txt_fields_completed(fields: Array[String]): Int = {
    if (fields(14)(0) == '\\') Math.round(fields(14).toFloat * 7) else 0
  }

  def month_of_birth(fields: Array[String]): Int = {
    val dob = fields(7)
    if (dob.split("/").length > 1) {
      dob.split("/").head.toInt
    } else {
      dob.split("-")(1).toInt
    }
  }

  def country_match(fields: Array[String]): Int = {
    if (fields(3) == fields(4)) 1 else 0
  }

  val spamCountrySet = scala.collection.immutable.Set(
    "GH", "SN", "NG", "TR", "GM", "CI", "TG", "SD", "LR", "MY")

  def is_in_spam_country(fields: Array[String]): Int = {
    val country = fields(3)
    if (spamCountrySet.contains(country)) 1 else 0
  }

}


