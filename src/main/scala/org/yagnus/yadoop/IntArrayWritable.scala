/**
 * This implements Writable instead of extending ArrayWritable for efficiency
 * reasons.
 *
 *
 * TODO: use VInt storage
 *
 * @author hc.busy
 *
 */

package org.yagnus.yadoop
import java.io.DataInput
import java.io.DataOutput
import java.io.IOException
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.VIntWritable

@inline class IntArrayWritable extends Writable {
  var array: Array[Int] = null;

  def this(in: Array[Int]) = {
    this();
    set(in)
  }
  def this(values: Int*) = {
    this();
    set(values.toArray);
  }

  def set(values: Array[Int]) = {
    this.array = values;
  }

  def get: Array[Int] = array;
  def length: Long = array.length;

  override def readFields(in: DataInput) {

    val serializer = new VIntWritable();
    serializer.readFields(in);
    array = new Array[Int](serializer.get); // construct values
    for (i ← 0 until array.length) {
      serializer.readFields(in);
      array(i) = serializer.get;
    }
  }

  override def write(out: DataOutput) {
    val serializer = new VIntWritable();

    if (array == null) {
      serializer.set(0);
      serializer.write(out);
    } else {
      serializer.set(array.length);
      serializer.write(out);
      for (l ← array) {
        serializer.set(l);
        serializer.write(out);
      }
    }
  }

  def getIntArrayWritable = this;
  def toIntArrayWritable = this;
  def clear { array = null; }
  def reset { array = null; }
}
@inline final class AuxIntArrayWritable(input: Iterator[Int], len: Long) extends IntArrayWritable {
  override def write(out: DataOutput) {
    val serializer = new VIntWritable();
    serializer.set(len.intValue);
    serializer.write(out);
    for (l ← input) {
      serializer.set(l);
      serializer.write(out);
    }
  }
  override def length = len;
}