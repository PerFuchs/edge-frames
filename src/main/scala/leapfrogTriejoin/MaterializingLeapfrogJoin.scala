package leapfrogTriejoin

class MaterializingLeapfrogJoin(iterators: Array[LinearIterator]) extends LeapfrogJoinInterface {
  private[this] val MATERIALIZATION_THRESHOLD = 200

  private[this] val materializable = false//iterators.forall(_.isInstanceOf[MaterializedIterator])
  private[this] var materialized = false

  private[this] val matIterators: Array[MaterializedIterator] = if (materializable) {
    iterators.map(_.asInstanceOf[MaterializedIterator])
  } else {
    null
  }

  private[this] val generalLeapFrogJoin = new LeapfrogJoin(iterators)

  private[this] var position: Int = 0

  private[this] var values: Array[Long] = null
  private[this] var positions: Array[Array[Int]] = null

  private def materialize(): Unit = {
    val smallestIterator = getSmallestIterator

    val maxIntersectionSize = smallestIterator.estimateSize

    position = 0
    values = new Array(maxIntersectionSize + 1)  // Plus setinel -1
    positions = new Array(maxIntersectionSize)  // Plus setinel -1
    var otherPositions: Array[Int] = new Array(matIterators.length)

    var i = 0
    val offset = smallestIterator.getPosition
    while (i < maxIntersectionSize) {
      val key = smallestIterator.get(i + offset) // TODO can jump positions here

      var j = 0
      var possiblyIn = true
      while (possiblyIn && j < matIterators.length) {  // TODO differiante between small and large iterators here
        if (matIterators(j) ne smallestIterator) {
          val leastUpperBoundPos = matIterators(j).idempotentSeek(key)
          if (leastUpperBoundPos < matIterators(j).getEnd)
          otherPositions(j) = leastUpperBoundPos
          possiblyIn = matIterators(j).get(leastUpperBoundPos) == key
        } else {
          otherPositions(j) = i + offset
        }
        j += 1
      }

      if (possiblyIn) {
        values(position) = key
        positions(position) = otherPositions
        otherPositions = new Array(matIterators.length)

        position += 1
      }

      i += 1
    }

    values(position) = -1

    position = 0
  }

  private def getSmallestIterator: MaterializedIterator = {
    var smallestIterator: MaterializedIterator = null
    var minSize = Integer.MAX_VALUE
    var i = 0
    while (i < matIterators.length) {
      val iter = matIterators(i)
      val size = iter.estimateSize
      if (size < minSize) {
        smallestIterator = iter
        minSize = size
      }
      i += 1
    }
    smallestIterator
  }

  private def setIteratorPositions(): Unit = {
    val positionsToSet = positions(position)
    assert(positionsToSet != null, s"positionsToSet is zero at $position")
    var i = 0
    while (i < matIterators.length) {
      matIterators(i).setPosition(positionsToSet(i))
      i += 1
    }
  }

  override def init(): Unit = {
    if (materializable) {
      materialized = false
      var i = 0
      while (i < iterators.length) {
        materialized |= matIterators(i).estimateSize < MATERIALIZATION_THRESHOLD
        i += 1
      }
    }

    if (materialized) {
      materialize()
      if (!atEnd) {
        setIteratorPositions()
      }
    } else {
      generalLeapFrogJoin.init()
    }
  }

  override def leapfrogNext(): Unit = {
    if (materialized) {
      position += 1
      if (!atEnd) {
        setIteratorPositions()
      }
    } else {
      generalLeapFrogJoin.leapfrogNext()
    }
  }

  override def leapfrogSeek(key: Long): Unit = {
    if (materialized) {
??? // Never used apparently, I'll not implement it
    } else {
      generalLeapFrogJoin.leapfrogNext()
    }
  }

  override def atEnd: Boolean = {
    if (materialized) {
      values(position) == -1
    } else {
      generalLeapFrogJoin.atEnd
    }
  }

  override def key: Long = {
    if (materialized) {
      values(position)
    } else {
      generalLeapFrogJoin.key
    }
  }
}
