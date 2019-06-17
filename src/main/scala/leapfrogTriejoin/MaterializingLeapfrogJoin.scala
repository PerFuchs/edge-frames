package leapfrogTriejoin

import experiments.InternalAnalysis

import scala.collection.mutable

/**
  * Optimized Leapfrog join for small iterators and intersections.
  *
  * If the resulting intersections are small, it is faster to materialize them into an array all at once
  * and return results from this buffer on every next call, due to better locality.
  *
  * This optimized version materializes the intersection of 2nd level iterators (usual very small in graphs)
  * and intersects the intersection of 2nd level iterators with the 1st level iterators in the next call.
  * This is a conscious design decission because the 1st level iterators need to be moved to the right
  * position on every next call anyhow - hence they need to be touched.
  *
  * We use iterative 2-way in-tandem intersection building to intersect the 2nd level itertors.
  * We start this process with the smallest interator to limit the intesection as much as possible from the
  * beginning.
  *
  * @param iterators
  */
class MaterializingLeapfrogJoin(var iterators: Array[TrieIterator]) extends LeapfrogJoinInterface {
  private[this] val DELETED_VALUE = -2

  if (iterators.isEmpty) {
    throw new IllegalArgumentException("iterators cannot be empty")
  }

  private[this] var isAtEnd: Boolean = false
  private[this] var keyValue = 0L

  private[this] var initialized = false

  private[this] var firstLevelIterators: Array[TrieIterator] = null
  private[this] var secondLevelIterators: Array[TrieIterator] = null

  private[this] var materializedValues: Array[Long] = new Array[Long](200)

  private[this] var position = 0

  private[this] var fallback: LeapfrogJoin = null

  def init(): Unit = {
    if (!initialized) {
      firstLevelIterators = iterators.filter(_.getDepth == 0)
      secondLevelIterators = iterators.filter(_.getDepth == 1)
      initialized = true

      if (secondLevelIterators.length == 0 || !MaterializingLeapfrogJoin.shouldMaterialize) {
        fallback = new LeapfrogJoin(iterators.map(_.asInstanceOf[LinearIterator]))
      }
    }

    InternalAnalysis.analyseIntersections(secondLevelIterators.map(_.clone().asInstanceOf[TrieIterator]))

    if (secondLevelIterators.length == 0 || !MaterializingLeapfrogJoin.shouldMaterialize) {
      fallback.init()
      isAtEnd = fallback.atEnd
      if (!isAtEnd) {
        keyValue = fallback.key
      }
    } else {
      iteratorAtEndExists()

      keyValue = -1

      if (!isAtEnd) {
        materialize()
        position = -1
        if (!isAtEnd) {
          leapfrogNext()
        }
      }
    }
  }

  private def materialize(): Unit = {
    if (secondLevelIterators.length == 1) {
      if (materializedValues.length < secondLevelIterators(0).estimateSize + 1) {
        materializedValues = new Array[Long](secondLevelIterators(0).estimateSize + 1)
      }
      materializeSingleIterator(secondLevelIterators(0))
    } else {
      moveSmallestIteratorFirst()
      if (materializedValues.length < secondLevelIterators(0).estimateSize + 1) {
        materializedValues = new Array[Long](secondLevelIterators(0).estimateSize + 1)
      }

      intersect(secondLevelIterators(0), secondLevelIterators(1))

      var i = 2
      while (i < secondLevelIterators.length) {
        intersect(secondLevelIterators(i))
        i += 1
      }
    }
    isAtEnd = materializedValues(0) == -1
  }

  @inline
  private def moveSmallestIteratorFirst(): Unit = {
    var i = 0
    var smallestSize = Integer.MAX_VALUE
    var smallestPosition = 0
    while (i < secondLevelIterators.length) {
      val size = secondLevelIterators(i).estimateSize
      if (size < smallestSize) {
        smallestSize = size
        smallestPosition = i
      }
      i += 1
    }

    val temp = secondLevelIterators(0)
    secondLevelIterators(0) = secondLevelIterators(smallestPosition)
    secondLevelIterators(smallestPosition) = temp
  }

  private def materializeSingleIterator(iter: LinearIterator): Unit = {
    var i = 0
    while (!iter.atEnd) {
      materializedValues(i) = iter.key
      iter.next()
      i += 1
    }
    materializedValues(i) = -1
  }

  // TODO reused of intersection! see print of mat after first and second intersection

  private def intersect(i1: LinearIterator, i2: LinearIterator): Unit = {
    var valueCounter = 0
    while (!i1.atEnd && !i2.atEnd) {
      if (i1.key == i2.key) {
        materializedValues(valueCounter) = i1.key
        valueCounter += 1
        i1.next()
        i2.next()
      } else if (i1.key < i2.key) {
        i1.seek(i2.key)
      } else {
        i2.seek(i1.key)
      }
    }
    materializedValues(valueCounter) = -1
  }

  private def intersect(iter: LinearIterator): Unit = { // TODO optimizable?

//    val buffer = mutable.Buffer[Long]()
//    val clone = iter.clone()
//    while (!clone.atEnd) {
//      buffer.append(clone.key)
//      clone.next()
//    }
//    val expected = materializedValues.intersect(buffer)

//    val before = new Array[Long](materializedValues.length)
//    materializedValues.copyToArray(before)

    var i = 0
    var value = materializedValues(i)
    while (value != -1 && !iter.atEnd) {
      if (value != DELETED_VALUE) {
        iter.seek(value)
        if (iter.key != value) {
          materializedValues(i) = DELETED_VALUE
        }
      }
      i += 1
      value = materializedValues(i)
    }

    while (materializedValues(i) != -1) {
      materializedValues(i) = DELETED_VALUE
      i += 1
    }


//    if (!(materializedValues.filter(_ != DELETED_VALUE).takeWhile(_ != -1) sameElements expected)) {
//      println("is", materializedValues.mkString(", "), "but should be", expected.mkString(", ") )
//      println("before", before.mkString(", "))
//      println("buffer", buffer.mkString(", "))
//    }

  }

  @inline
  private def iteratorAtEndExists(): Unit = {
    isAtEnd = false
    var i = 0
    while (i < iterators.length) {
      if (iterators(i).atEnd) {
        isAtEnd = true
      }
      i += 1
    }
  }

  def leapfrogNext(): Unit = {
    if (fallback == null) {
      var found = false

      do {
        position += 1
        keyValue = materializedValues(position)
        if (keyValue >= 0) {
          found |= filterAgainstFirstLevelIterators(keyValue)
        } else if (keyValue == -1) {
          isAtEnd = true
        }
      } while (!found && !isAtEnd)
    } else {
      fallback.leapfrogNext()
      isAtEnd = fallback.atEnd
      if (!isAtEnd) {
        keyValue = fallback.key
      }
    }
  }

  @inline
  private def filterAgainstFirstLevelIterators(value: Long): Boolean = {
    var i = 0
    var in = true
    while (!isAtEnd && i < firstLevelIterators.length) {
      if (!firstLevelIterators(i).seek(value)) {  // TODO can i optimize that for the case that it is nearly always true?
        in &= firstLevelIterators(i).key == value
      } else {
        isAtEnd = true
      }
      i += 1
    }
    in
  }

  override def key: Long = keyValue

  override def atEnd: Boolean = isAtEnd
}

object MaterializingLeapfrogJoin {
  private var shouldMaterialize = true

  def setShouldMaterialize(value: Boolean): Unit ={
    if (value) {
      println("Leapfrogjoins ARE materialized")
    } else {
      println("Leapfrogjoins are NOT materialized")
    }
    shouldMaterialize = value
  }
}
