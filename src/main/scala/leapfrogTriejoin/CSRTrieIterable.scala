package leapfrogTriejoin

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import partitioning.shares.Hypercube
import partitioning.{Partitioning, SharesRange}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// TODO serializing could be improved by sending common parts of the array only once
class CSRTrieIterable(private[this] val verticeIDs: Array[Long],
                      val edgeIndices: Array[Int],
                      private[this] val edges: Array[Long]) extends TrieIterable with Serializable {

  override def trieIterator: TrieIteratorImpl = {
    trieIterator(None, None, None, None)
  }

  def trieIterator(partition: Int,
                   partitioning: Partitioning,
                   dimensionFirstLevel: Int,
                   dimensionSecondLevel: Int): TrieIteratorImpl = {
    new TrieIteratorImpl(Option(partition), Option(partitioning), Option(dimensionFirstLevel), Option(dimensionSecondLevel))
  }


  def trieIterator(partition: Option[Int],
                   partitioning: Option[Partitioning],
                   dimensionFirstLevel: Option[Int],
                   dimensionSecondLevel: Option[Int]): TrieIteratorImpl = {
    new TrieIteratorImpl(partition, partitioning, dimensionFirstLevel, dimensionSecondLevel)
  }

  var ranges: Map[Int, Seq[(Int, Int)]] = null

  // TODO currently I get different ranges for dst accesible TrieIterators because they come from different CSRTrieIterables for which
  //  one uses outdegrees and the other uses indegrees
  def initRanges(partitioning: SharesRange): Unit = {
        println("Init rangews")
    val start = System.nanoTime()
        ranges = partitioning.hypercube.get.dimensionSizes.zipWithIndex.map(
          d => (d._2, getOutdegreeBalancedRanges(d._1))).toMap
        println(s"Done init range: ${(System.nanoTime() - start) / 1e9} Ranges: ${ranges.mkString(", ")}")
  }

  def getOutdegreeBalancedRanges(numRanges: Int): Seq[(Int, Int)] = {
    val upper = edgeIndices.length - 1

    val totalOutdegree = edgeIndices(edgeIndices.length - 1)
    val outdegreePerRange = totalOutdegree / numRanges


    val ranges = mutable.Buffer[(Int, Int)]()
    var i = 0
    var currentOffsetSum = 0
    var currentRangeStart = 0
    while (i < upper) {
      currentOffsetSum += edgeIndices(i)
      if (currentOffsetSum > outdegreePerRange) {
        ranges.append((currentRangeStart, i + 1))
        currentRangeStart = i + 1
        currentOffsetSum = 0
      }
      i += 1
    }
    if (ranges.last._2 != upper) {
      ranges.append((currentRangeStart, upper))
    }
    require(ranges.size == numRanges)
    ranges

    //
    //    val ranges = (0 until numRanges).map(i => (i * rangeSize, if (i == numRanges - 1) {
    //      upper
    //    } else {
    //      (i + 1) * rangeSize
    //    })).toArray
    //
    //    var maxRangeIndex = getMaxRange(ranges)
    //    var minRangeIndex = getMinRange(ranges)
    //
    ////    println(ranges.mkString(", "))
    //
    //    val limit = 0.99 // 1% size difference aim
    //
    //    while (getRangeSize(ranges(minRangeIndex)).toDouble / getRangeSize(ranges(maxRangeIndex)) < limit ) {
    ////      println(getRangeSize(ranges(minRangeIndex)).toDouble / getRangeSize(ranges(maxRangeIndex)))
    //      balanceRanges(ranges, minRangeIndex, maxRangeIndex)
    //      maxRangeIndex = getMaxRange(ranges)
    //      minRangeIndex = getMinRange(ranges)
    //    }
    ranges
  }

  //  private def balanceRanges(ranges: Array[(Int, Int)], minRangeIndex: Int, maxRangeIndex: Int): Unit = {
  //    val differenceHalf = (getRangeSize(ranges(maxRangeIndex)) - getRangeSize(ranges(minRangeIndex))) / 2
  //
  ////    println(differenceHalf)
  //
  //    assert(differenceHalf< getRangeSize(ranges(minRangeIndex)))
  //    if (minRangeIndex < maxRangeIndex) {
  //      var i = ranges(minRangeIndex)._2
  //      do {
  //        i += 1
  //      } while(edgeIndices(i) - edgeIndices(ranges(minRangeIndex)._2) < differenceHalf)
  //      i -= 1  // Smaller difference not equal or bigger
  //
  //      val adjustment = i - ranges(minRangeIndex)._2
  //
  //
  //      ranges(minRangeIndex) = (ranges(minRangeIndex)._1, ranges(minRangeIndex)._2 + adjustment)
  //
  //      i = minRangeIndex + 1
  //      while(i < maxRangeIndex) {
  //        ranges(i) = (ranges(i)._1 + adjustment, ranges(i)._2 + adjustment)
  //        i += 1
  //      }
  //      ranges(maxRangeIndex) = (ranges(maxRangeIndex)._1 + adjustment, ranges(maxRangeIndex)._2)
  //    } else if (maxRangeIndex < minRangeIndex) {
  //      var i = ranges(minRangeIndex)._1
  ////      println("i", i)
  //      do {
  //        i -= 1
  ////        println("i", i)
  //      } while(edgeIndices(ranges(minRangeIndex)._1) - edgeIndices(i) < differenceHalf)
  //      i += 1  // Smaller difference not equal or bigger
  //
  //      val adjustment = ranges(minRangeIndex)._1 - i
  //
  //      ranges(minRangeIndex) = (ranges(minRangeIndex)._1 - adjustment, ranges(minRangeIndex)._2)
  //
  //      i = minRangeIndex - 1
  //      while(maxRangeIndex < i) {
  //        ranges(i) = (ranges(i)._1 - adjustment, ranges(i)._2 - adjustment)
  //        i -= 1
  //      }
  //      ranges(maxRangeIndex) = (ranges(maxRangeIndex)._1, ranges(maxRangeIndex)._2 - adjustment)
  //    }
  //  }

  private def getMaxRange(ranges: Seq[(Int, Int)]): Int = {
    var i = 0
    var maxRange = ranges.head
    var index = 0
    while (i < ranges.length) {
      if (getRangeSize(maxRange) < getRangeSize(ranges(i))) {
        maxRange = ranges(i)
        index = i
      }
      i += 1
    }
    index
  }

  private def getMinRange(ranges: Seq[(Int, Int)]): Int = {
    var i = 0
    var minRange = ranges.head
    var index = 0
    while (i < ranges.length) {
      if (getRangeSize(ranges(i)) < getRangeSize(minRange)) {
        minRange = ranges(i)
        index = i
      }
      i += 1
    }
    index
  }

  private def getRangeSize(range: (Int, Int)): Int = {
    edgeIndices(range._2) - edgeIndices(range._1)
  }


  // TODO sort out range filtering functionality, either in here or in MultiRangePartitionTrieIterator
  class TrieIteratorImpl(
                          val partition: Option[Int],
                          val partitioning: Option[Partitioning],
                          val dimensionFirstLevel: Option[Int],
                          val dimensionSecondLevel: Option[Int]
                        ) extends TrieIterator {

    /*
     Partitioning
     Allows to bind both levels of the iterator to a range of values. The lower bound is included the upper bound is excluded.
     For no range partitionings, we choose 0 and the last node index + 1 as range, which has no effect because this is the original
     lower and upper bound.
     */
    private[this] val (firstLevelLowerBound, firstLevelUpperBound, secondLevelLowerBound, secondLevelUpperBound) = partitioning
    match {
      case Some(SharesRange(Some(hypercube), _)) => {
        val upper = edgeIndices.length - 1
        val coordinate = hypercube.getCoordinate(partition.get)

        //        val (fl, fu) = ranges(dimensionFirstLevel.get)(coordinate(dimensionFirstLevel.get))
        //        val (sl, su) = ranges(dimensionSecondLevel.get)(coordinate(dimensionSecondLevel.get))
        val (fl, fu) = if (dimensionFirstLevel.get == 1) {
          getPartitionBoundsInRange(0, upper, coordinate(dimensionFirstLevel.get), hypercube.dimensionSizes(dimensionFirstLevel.get),
            true)
        } else {
          getPartitionBoundsInRange(0, upper, coordinate(dimensionFirstLevel.get), hypercube.dimensionSizes(dimensionFirstLevel.get),
            true)
        }
        val (sl, su) = if (dimensionSecondLevel.get == 1) {
          getPartitionBoundsInRange(0, upper, coordinate(dimensionSecondLevel.get), hypercube.dimensionSizes(dimensionSecondLevel.get),
            true)
        } else {
          getPartitionBoundsInRange(0, upper, coordinate(dimensionSecondLevel.get), hypercube.dimensionSizes(dimensionSecondLevel.get),
            true)
        }
        (fl, fu, sl, su)
      }
      case _ => {
        (0, edgeIndices.length - 1, 0, edgeIndices.length - 1)
      }
    }

    //    println(s"Partition: ${partition.get} bounds: ", firstLevelLowerBound, firstLevelUpperBound, secondLevelLowerBound,
    //      secondLevelUpperBound)

    private[this] var isAtEnd = verticeIDs.length == 0

    private[this] var depth = -1

    private[this] var srcPosition: Int = firstLevelLowerBound
    if (!isAtEnd && edgeIndices(srcPosition) == edgeIndices(srcPosition + 1)) {
      moveToNextSrcPosition()
    }
    isAtEnd = firstLevelUpperBound <= srcPosition

    private[this] val firstSourcePosition = srcPosition

    private[this] var dstPosition = 0

    private[this] var keyValue = 0L

    private def getPartitionBoundsInRange(lower: Int, upper: Int, partition: Int, numPartitions: Int, fromBelow: Boolean): (Int, Int) = {
      val totalSize = upper - lower
      val partitionSize = totalSize / numPartitions
      val lowerBound = if (fromBelow) {
        lower + partition * partitionSize
      } else {
        if (partition == numPartitions - 1) {
          lower
        } else {
          upper - (partition + 1) * partitionSize
        }
      }


      val upperBound = if (fromBelow) {
        if (partition == numPartitions - 1) {
          upper
        } else {
          lower + (partition + 1) * partitionSize
        }
      } else {
        upper - partition * partitionSize
      }
      (lowerBound, upperBound)
    }

    override def open(): Unit = {
      assert(!isAtEnd, "open cannot be called when atEnd")
      depth += 1

      if (depth == 0) {
        srcPosition = firstSourcePosition
        keyValue = srcPosition.toLong
      } else if (depth == 1) { // TODO predicatable
        dstPosition = edgeIndices(srcPosition)
        isAtEnd = secondLevelUpperBound <= edges(dstPosition)
        if (!isAtEnd && secondLevelLowerBound != 0) {
          seek(secondLevelLowerBound)
        } else {
          keyValue = edges(dstPosition)
        }
      }

      assert(!isAtEnd, "open cannot be called when atEnd")
      assert(depth < 2)
    }

    override def up(): Unit = {
      assert(depth == 1 || depth == 0, s"Depth was $depth")
      depth -= 1
      isAtEnd = false
      if (depth == 0) {
        keyValue = srcPosition
      }
    }

    override def key: Long = {
      keyValue
    }

    override def next(): Unit = {
      assert(!atEnd)
      if (depth == 0) {
        moveToNextSrcPosition()
        isAtEnd = firstLevelUpperBound <= srcPosition
        keyValue = srcPosition.toLong
      } else {
        dstPosition += 1
        isAtEnd = dstPosition == edgeIndices(srcPosition + 1) || secondLevelUpperBound <= edges(dstPosition)
        // edgeIndices(srcPosition + 1) should not be factored out, it does not look like this improves performance (looks!)
        if (!isAtEnd) {
          keyValue = edges(dstPosition)
        }
      }
    }

    override def atEnd: Boolean = {
      isAtEnd
    }

    override def seek(key: Long): Boolean = {
      assert(!atEnd)
      // TODO this quickfix is problematic when I use the internal range partitioning
      //      if (keyValue < key) { // TODO quickfix, why does this call even happen, clique3 either amazon0302 or liveJournal
      assert(keyValue < key)
      if (depth == 0) {
        srcPosition = key.toInt
        if (srcPosition < edgeIndices.length - 1 && // TODO srcPosition should never be bigger than edgeIndices.lenght -  1, investigate
          edgeIndices(srcPosition) == edgeIndices(srcPosition + 1)) { // TODO does srcPosition < edgeIndices.length - 1 ruin
          // predicatability?
          moveToNextSrcPosition()
        }
        isAtEnd = firstLevelUpperBound <= srcPosition
        keyValue = srcPosition.toLong
        isAtEnd
      } else {
        dstPosition = ArraySearch.find(edges, key, dstPosition, edgeIndices(srcPosition + 1))
        isAtEnd = dstPosition == edgeIndices(srcPosition + 1) || secondLevelUpperBound <= edges(dstPosition)
        if (!isAtEnd) {
          keyValue = edges(dstPosition)
        }
      }
      //      }
      isAtEnd
    }

    private def moveToNextSrcPosition(): Unit = {
      var indexToSearch = edgeIndices(srcPosition + 1) // A linear search is ideal, see log 05.06

      do {
        srcPosition += 1
      } while (srcPosition < edgeIndices.length - 1 && edgeIndices(srcPosition + 1) == indexToSearch) // TODO sentry element
    }

    // For testing
    def translate(key: Int): Long = {
      verticeIDs(key)
    }

    def translate(keys: Array[Long]): Array[Long] = {
      var i = 0
      while (i < keys.length) {
        keys(i) = verticeIDs(keys(i).toInt)
        i += 1
      }
      keys
    }

    override def estimateSize: Int = {
      if (depth == 0) {
        Integer.MAX_VALUE
      } else {
        edgeIndices(srcPosition + 1) - edgeIndices(srcPosition)
      }
    }

    override def getDepth: Int = {
      depth
    }

    override def clone(): AnyRef = {
      val c = new TrieIteratorImpl(partition, partitioning, dimensionFirstLevel, dimensionSecondLevel)
      c.copy(isAtEnd, depth, srcPosition, dstPosition, keyValue)
      c
    }

    private def copy(atEnd: Boolean, depth: Int, srcPosition: Int, dstPosition: Int, keyValue: Long) {
      isAtEnd = atEnd
      this.depth = depth
      this.srcPosition = srcPosition
      this.dstPosition = dstPosition
      this.keyValue = keyValue
    }
  }

  override def iterator: Iterator[InternalRow] = {
    var currentSrcPosition = 0
    var currentDstPosition = 0

    new Iterator[InternalRow] {
      override def hasNext: Boolean = {
        currentDstPosition < edges.length
      }

      override def next(): InternalRow = {
        while (currentDstPosition >= edgeIndices(currentSrcPosition + 1)) {
          currentSrcPosition += 1
        }
        val r = InternalRow(verticeIDs(currentSrcPosition), verticeIDs(edges(currentDstPosition).toInt))
        currentDstPosition += 1
        r
      }
    }
  }

  override def memoryUsage: Long = {
    verticeIDs.length * 8 + edgeIndices.length * 4 + edges.length * 8
  }

  // For testing
  def getVerticeIDs: Array[Long] = {
    verticeIDs
  }

  // For testing
  def getTranslatedEdges: Array[Long] = {
    edges.map(ei => verticeIDs(ei.toInt))
  }

  // For testing
  def getEdgeIndices: Array[Int] = {
    edgeIndices
  }

  def minValue: Int = {
    0
  }

  def maxValue: Int = {
    edgeIndices.length - 1
  }
}


object CSRTrieIterable {
  def buildBothDirectionsFrom(iterSrcDst: Iterator[InternalRow], iterDstSrc: Iterator[InternalRow]): (CSRTrieIterable, CSRTrieIterable) = {
    if (iterSrcDst.hasNext) {
      val alignedZippedIter = new AlignedZippedIterator(iterSrcDst, iterDstSrc).buffered

      val verticeIDsBuffer = new ArrayBuffer[Long](10000)
      val edgeIndicesSrcBuffer = new ArrayBuffer[Int](10000) // TODO those two are the same.
      val edgeIndicesDstBuffer = new ArrayBuffer[Int](10000)
      val edgesDstBuffer = new ArrayBuffer[Long](10000)
      val edgesSrcBuffer = new ArrayBuffer[Long](10000)

      val verticeIDToIndex = new mutable.HashMap[Long, Long]()

      var lastVertice = alignedZippedIter.head(0)

      edgeIndicesSrcBuffer.append(0)
      edgeIndicesDstBuffer.append(0)

      alignedZippedIter.foreach(a => {
        val nextVertice = a(0)
        if (lastVertice != nextVertice) {
          edgeIndicesSrcBuffer.append(edgesDstBuffer.size)
          edgeIndicesDstBuffer.append(edgesSrcBuffer.size)

          verticeIDToIndex.put(lastVertice, verticeIDsBuffer.size)

          verticeIDsBuffer.append(lastVertice)

          lastVertice = nextVertice
        }
        if (a(1) != -1) {
          edgesDstBuffer.append(a(1))
        }
        if (a(2) != -1) {
          edgesSrcBuffer.append(a(2))
        }
      })

      edgeIndicesSrcBuffer.append(edgesDstBuffer.size)
      edgeIndicesDstBuffer.append(edgesSrcBuffer.size)

      verticeIDToIndex.put(lastVertice, verticeIDsBuffer.size)

      verticeIDsBuffer.append(lastVertice)

      // TODO Optimize
      val edgesDstArray = edgesDstBuffer.toArray.map(dst => verticeIDToIndex(dst))
      val edgesSrcArray = edgesSrcBuffer.toArray.map(src => verticeIDToIndex(src))

      val verticeIDs = verticeIDsBuffer.toArray

      (new CSRTrieIterable(verticeIDs, edgeIndicesSrcBuffer.toArray, edgesDstArray), new CSRTrieIterable(verticeIDs, edgeIndicesDstBuffer.toArray,
        edgesSrcArray))
    } else {
      (new CSRTrieIterable(Array[Long](), Array[Int](), Array[Long]()),
        new CSRTrieIterable(Array[Long](), Array[Int](), Array[Long]()))
    }
  }

  // For testing
  def buildBothDirectionsFrom(srcDst: Array[(Long, Long)], dstSrc: Array[(Long, Long)]): (CSRTrieIterable, CSRTrieIterable) = {
    buildBothDirectionsFrom(srcDst.map(t => new GenericInternalRow(Array[Any](t._1, t._2))).iterator,
      dstSrc.map(t => new GenericInternalRow(Array[Any](t._1, t._2))).iterator)
  }
}