package org.yagnus.yadoop
import java.io.File
import java.net.URI

import org.apache.hadoop.fs.{ Path, FileStatus }
import org.apache.hadoop.io.{ UTF8, Text, LongWritable, IntWritable, FloatWritable, BytesWritable, BooleanWritable }

/**
 * The idea of using implicit to simplify implementing hadoop using scala came from a package called SHadoop on google code.
 *
 * Some improvements, renaming, and corruption has occurred since. This package is developed with the express purpose of
 * support Yagnus's hzip project.
 *
 * @author hc.busy
 *
 */
object Yadoop {

  //TODO rewrite Write BytesWritable to take TraversableOnce[Byte], because below would still perform a copy on any slices we pass in.
  implicit def b2bw(value: TraversableOnce[Byte]) = { new BytesWritable(value.toArray); }

  implicit def intArrayWritable2IntArray(value: IntArrayWritable) = value.get;
  implicit def intArray2IntArrayWritable(values: Array[Int]): IntArrayWritable = new IntArrayWritable(values);
  implicit def intSeq2IntArrayWritable(values: Seq[Int]): IntArrayWritable = new IntArrayWritable(values.toArray);
  implicit def intIterableLong2IntArrayWritable(in: (Iterator[Int], Long)): IntArrayWritable = new AuxIntArrayWritable(in._1, in._2);
  implicit def intIterableInt2IntArrayWritable(in: (Iterator[Int], Int)): IntArrayWritable = new AuxIntArrayWritable(in._1, in._2);
  implicit def intIterableInt2IntArrayWritable(in: Iterator[Int]): IntArrayWritable = new AuxIntArrayWritable(in, in.size);

  implicit def doubleArrayWritable2DoubleArray(value: DoubleArrayWritable): Array[Double] = value.get;
  implicit def doubleArray2DoubleArrayWritable(values: Array[Double]): DoubleArrayWritable = new DoubleArrayWritable(values);
  implicit def doubleSeq2DoubleArrayWritable(values: Seq[Double]): DoubleArrayWritable = new DoubleArrayWritable(values.toArray);

  implicit def w2i(value: IntWritable) = value.get
  implicit def i2w(value: Int) = new IntWritable(value)

  implicit def w2l(value: LongWritable) = value.get
  implicit def l2w(value: Long) = new LongWritable(value)
  implicit def w2b(value: BooleanWritable) = value.get
  implicit def b2w(value: Boolean) = new BooleanWritable(value)

  implicit def w2f(value: FloatWritable) = value.get
  implicit def f2w(value: Float) = new FloatWritable(value)

  implicit def t2s(value: Text) = value.toString
  implicit def s2t(value: String) = new Text(value)

  implicit def u82s(value: UTF8) = value.toString
  implicit def s2u8(value: String) = new UTF8(value)

  implicit def pathStrOfPath(p: Path): String = p.getPath;
  implicit def pathStrOfFile(f: File): String = f.getPath;
  implicit def pathStrOfFileStatus(fs: FileStatus): String = fs.getPath.getPath;

  implicit def pathUriOfPath(p: Path): URI = p.toUri;
  implicit def pathUriOfFile(p: File): URI = p.toUri;
  implicit def pathUriOfFileStatus(fs: FileStatus): URI = fs.toUri;

  implicit def pathOf(value: String) = new Path(value)
  implicit def pathOfFile(p: File): Path = p.getPath
  implicit def pathOfFileStatys(fs: FileStatus): Path = fs.getPath

  implicit def b2bw(value: Array[Byte]) = new BytesWritable(value);
  implicit def bw2b(value: BytesWritable): Array[Byte] = value.getBytes;

  /**
   * *
   * For the people who got bit with memory problems when they wrote an array of bytes out that resulted from the above code, you may want to type out
   * the method name more explicitly to be sure that the right object is being implicitly constructed.
   */
  final class SkinTightBytesWritable(theBytes: BytesWritable) {
    def exactByteArray = theBytes.getBytes.slice(0, theBytes.getLength);
    def exactByteIterable = theBytes.getBytes.view(0, theBytes.getLength);
    def wholeBytesBuffer = theBytes.getBytes
  }
  implicit def bw2b2(value: BytesWritable): SkinTightBytesWritable = new SkinTightBytesWritable(value);

  /**
   * these methods converts a List[Path] to List[String] representing those paths.
   * @param paths
   * @return the same paths as strings
   */
  implicit def convertsFileStatusToStringNames(paths: List[FileStatus]): List[String] = paths.map(pathStrOfFileStatus);
  implicit def convertsFileStatusToStringNames(paths: Seq[FileStatus]): Seq[String] = paths.map(pathStrOfFileStatus);
  implicit def convertsFileStatusToPath(paths: Seq[FileStatus]): Seq[Path] = paths.map(_.getPath);

}
