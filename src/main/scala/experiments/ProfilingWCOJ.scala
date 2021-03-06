package experiments

import java.lang.management.ManagementFactory

import leapfrogTriejoin.{ArrayTrieIterable, EdgeRelationship, LeapfrogTriejoin, TrieIterator}
import leapfrogTriejoin.implicits._
import partitioning.AllTuples

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.collection.JavaConverters._

object ProfilingWCOJ extends App {

  val DATASET_PATH = "/home/per/workspace/master-thesis/datasets/amazon-0302.csv"
  val REPS = 8

  val ds: Array[(Long, Long)] = loadDataset(DATASET_PATH)

 // Five clique
//  val edges: List[EdgeRelationship] = ('a' to 'e')
//    .combinations(2)
//    .filter(l => l(0) < l(1))
//    .map(l => new EdgeRelationship((s"${l(0)}", s"${l(1)}")))
//    .toList

  // 6 cycle query
  var edges: mutable.Buffer[EdgeRelationship] = ('a' to 'f')
      .sliding(2)
      .toList
      .map(l => new EdgeRelationship((s"${l(0)}", s"${l(1)}")))
      .toBuffer
  edges.append(new EdgeRelationship("a", "f"))
  println(edges.map(_.variables.mkString(", ")).mkString("\n"))

  val times = mutable.ListBuffer[Double]()
//  for (rep <- 0 until REPS) {
    val rels: List[TrieIterator] = edges.toList
      .map(e => new ArrayTrieIterable(ds).trieIterator)
    val join = new LeapfrogTriejoin(edges.zip(rels).toMap, Seq("a", "b", "c", "d", "e"), partition = 0, partitioning = AllTuples())
    scala.io.StdIn.readLine("Ready. Continue?")
    doJoin(join)
//  }
  println(s"Average of $REPS repetitions: ${Utils.avg(times)}")

  def doJoin(join: LeapfrogTriejoin) = {
    System.gc()
    Utils.getGarbageCollectionStatsSinceLastCall()
    val start = System.nanoTime()
    var i = 0
    while (!join.atEnd) {
      join.next()
      i += 1
    }
    val end = System.nanoTime()

    val gcStats = Utils.getGarbageCollectionStatsSinceLastCall()

    val time = (end - start).toDouble / 1000000000

    println(s"GC count: ${gcStats.count}, GC time: ${gcStats.time}")
    println(s"Result size: ${i}, $time")
    times.append(time)
  }

  def loadDataset(datasetPath: String): Array[(Long, Long)] = {
    val output = new ListBuffer[(Long, Long)]()
    val bufferedSource = Source.fromFile(datasetPath)
    for (line <- bufferedSource.getLines) {
      if (!line.startsWith("#")) {
        val cols = line.split("\t")
        require(cols.size == 2)
        output.append((cols(0).toLong, cols(1).toLong))
      }
    }
    bufferedSource.close
    output.toArray
  }

}
