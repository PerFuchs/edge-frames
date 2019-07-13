package leapfrogTriejoin

import scala.collection.mutable

class LeapfrogJoin(var iterators: Array[LinearIterator]) extends LeapfrogJoinInterface {
  if (iterators.isEmpty) {
    throw new IllegalArgumentException("iterators cannot be empty")
  }

  var atEnd: Boolean = false
  private[this] var p = 0
  var key = 0L

  def init(): Unit = {
    iteratorAtEndExists()

    p = 0
    key = 0

    if (!atEnd) {
      //      println(iterators.map(_.estimateSize).mkString(", "))
      //      test(iterators.map(_.clone().asInstanceOf[LinearIterator]))
      sortIterators()
      leapfrogSearch()
    }
    //    println(atEnd)
  }

  def test(iterators: Array[LinearIterator]): Unit = {
    println("Best estimate", iterators.map(_.estimateSize).mkString(", "))
    val materialized = iterators.map(toList)
    println("Total intersection", intersect(materialized.toList).length)
    println("Intersection between two", materialized.combinations(2)
      .map(a => ((a.head.length, a(1).length), intersect(a.toList).length)).mkString(", "))
    println()
  }

  def toList(i: LinearIterator): Array[Long]=  {
    val values = mutable.Buffer[Long]()
    while (!i.atEnd) {
      i.next()
      if (!i.atEnd) {
        values.append(i.key)
      }
    }
    values.toArray
  }

  def intersect(values: List[Array[Long]]): Array[Long] = {
    values match {
      case Nil => ???
      case x :: Nil => x
      case x :: xs => x.intersect(intersect(xs))
    }
  }

  @inline
  private def iteratorAtEndExists(): Unit = {
    atEnd = false
    var i = 0
    while (i < iterators.length) {
      if (iterators(i).atEnd) {
        atEnd = true
      }
      i += 1
    }
  }

  // sorts the iterator such that p points to the smallest iterator and p - 1 to the largest
  // Public for testing, returns p for testing
  def sortIterators(): Int = {
    var i = 1
    while (i < iterators.length) {
      val iteratorToSort = iterators(i)
      val keyToSort = iterators(i).key
      var j = i
      while (j > 0 && iterators(j - 1).key > keyToSort) {
        iterators(j) = iterators(j - 1)
        j -= 1
      }
      iterators(j) = iteratorToSort
      i += 1
    }
    0
  }

  private def leapfrogSearch(): Unit = {
    var max = iterators(if (p > 0) {
      p - 1
    } else {
      iterators.length - 1
    }).key

    var min = iterators(p).key

    while (min != max && !iterators(p).seek(max)) {
      max = iterators(p).key

      if (p < iterators.length - 1) {
        p += 1
      } else {
        p = 0
      }
      min = iterators(p).key
    }
    key = min
    atEnd = iterators(p).atEnd
  }

  def leapfrogNext(): Unit = {
    iterators(p).next()
    if (iterators(p).atEnd) {
      atEnd = true
    } else {
      if (p < iterators.length - 1) {
        p += 1
      } else {
        p = 0
      }
      leapfrogSearch()
    }
  }

  def leapfrogSeek(key: Long): Unit = {
    iterators(p).seek(key)
    if (iterators(p).atEnd) {
      atEnd = true
    } else {
      if (p < iterators.length - 1) {
        p += 1
      } else {
        p = 0
      }
      leapfrogSearch()
    }
  }
}
