package leapfrogTriejoin

abstract class LeapfrogJoinInterface {

  def init(): Unit
  def leapfrogNext(): Unit
  def leapfrogSeek(key: Int): Unit
  def key: Int
  def atEnd: Boolean
}
