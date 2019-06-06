package leapfrogTriejoin

trait MaterializedIterator {

  /**
    * Estimates the size of the linear iterator, can return a size which is too high but
    * never a size smaller than the actual size.
    * The size of an iterator is defined by the number of legal next calls.
    *
    * @return the estimate
    */
  def estimateSize: Int

  def get(pos: Int): Int

  def idempotentSeek(key: Int): Int

  // TODO dangerous, I want it without in the end
  def setPosition(pos: Int)

  def getPosition: Int

  def getEnd: Int

}
