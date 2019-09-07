package leapfrogTriejoin

/**
  * An OnHeapColumnVector variant that exposes it's internal array.
  */
class ExposedArrayColumnVector(capacity: Int) extends WritableLongColumnVector(capacity) {
  var longData = new Array[Int](capacity)

  override def getInt(i: Int): Int = longData(i)

  override def reserveInternal(newCapacity: Int): Unit = {
    if (longData.length < newCapacity) {
      val newData = new Array[Int](newCapacity)
      System.arraycopy(longData, 0, newData, 0, capacity)
      longData = newData
    }
  }

  override def putInt(rowId: Int, value: Int): Unit = {
    longData(rowId) = value
  }

  override def putLong(i: Int, l: Long): Unit = {
    ???
  }

  override def getLong(i: Int): Long = ???
}
