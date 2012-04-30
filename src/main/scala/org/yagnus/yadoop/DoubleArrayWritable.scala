package org.yagnus.yadoop
import java.io.DataOutput
import org.apache.hadoop.io.WritableComparable
import java.io.DataInput

@inline class DoubleArrayWritable extends WritableComparable[DoubleArrayWritable] {
  var array: Array[Double] = null;

  var equalLengthOnly = true;

  def this(in: Array[Double]) {
    this();
    set(in);
  }

  def this(in: Double*) {
    this();
    set(in.toArray);
  }

  def set(values: Array[Double]) = {
    this.array = values;
  }

  def get: Array[Double] = array;

  override def readFields(in: DataInput) {
    array = new Array[Double](in.readInt); // construct values
    for (i ← 0 until array.length)
      array(i) = in.readDouble
  }

  override def write(out: DataOutput) {
    if (array == null) {
      out.writeInt(0);
    } else {
      out.writeInt(array.length);
      for (l ← array) {
        out.writeDouble(l);
      }
    }
  }

  override def equals(o: Any): Boolean = {
    if (o == null) return false;
    if (!o.isInstanceOf[DoubleArrayWritable]) return false;
    return compareTo(o.asInstanceOf[DoubleArrayWritable]) == 0;
  }
  override def compareTo(o: DoubleArrayWritable): Int = {
    if (o eq null) {
      if (equalLengthOnly) throw new DoubleArrayWritableError("DoubleArrayWritable.compaerTo received a null array to compare to.");
      return 1;
    }
    if (array.length == o.array.length) {
      for (i ← 0 until array.length) {
        if (this.array(i) > o.array(i)) {
          return 1
        } else if (this.array(i) < o.array(i)) {
          return -1;
        }
      }
      return 0;
    } else if (array.length > o.array.length) {
      if (equalLengthOnly) throw new DoubleArrayWritableError("DoubleArrayWritable.compaerTo received a comparison to a shorter array");
      return 1;
    } else {
      if (equalLengthOnly) throw new DoubleArrayWritableError("DoubleArrayWritable.compaerTo received a comparison to a longer array");
      return -1;
    }
  }
  def getDoubleArrayWritable = this;
  def toDoubleArrayWritable = this;
  def clear { array = null; }
  def reset { array = null; }
}

class DoubleArrayWritableError(x: String) extends Exception(x) {
}
