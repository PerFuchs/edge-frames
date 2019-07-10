package experiments

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.{Files, Paths}

import au.com.bytecode.opencsv.CSVWriter
import leapfrogTriejoin.{LinearIterator, TrieIterator}
import org.apache.spark.sql.WCOJFunctions

import scala.annotation.elidable
import scala.annotation.elidable._
import scala.collection.mutable

object InternalAnalysis {
  val ANALYSIS: Int = ASSERTION + 1

  val ANALYSIS_OUTPUT_FOLDER = "internal_analysis"
  val INTERSECTION_ANALYSIS_OUTPUT: String = Seq(ANALYSIS_OUTPUT_FOLDER, "intersections.csv").mkString("/")

  if (!Files.exists(Paths.get(ANALYSIS_OUTPUT_FOLDER))) {
    Files.createDirectories(Paths.get(ANALYSIS_OUTPUT_FOLDER))
  }
  var intersectionAnalysisWriter: CSVWriter = new CSVWriter(new BufferedWriter(new FileWriter(INTERSECTION_ANALYSIS_OUTPUT)), ',', 0)

  @elidable(ANALYSIS)
  def setupAnalysis(config: ExperimentConfig): Unit = {
    require(!config.algorithms.contains(experiments.WCOJ), "Analysis currently only works for GraphWCOJ.")

    intersectionAnalysisWriter.writeNext(Array("total", "smallestIterator", "smallestIteratorBiggest"))
  }

  @elidable(ANALYSIS)
  def analyseIntersections(iterators: Array[TrieIterator]): Unit = {
    if (iterators.nonEmpty) {

      val materialized: Array[Array[Long]] = iterators.map(linearIteratorToArray)

      if (materialized.length > 1) {
        val min = materialized.map(_.length).min
        val smallest = materialized.filter(_.length == min).head

        val smallestWithOthers = materialized.filter(_ ne smallest).map(smallest.intersect(_))
        val biggestAfterSmallest = smallestWithOthers.map(_.length).max
        val totalIntersection = intersect(smallestWithOthers)

        intersectionAnalysisWriter.writeNext(Array(totalIntersection.length.toString, smallest.length.toString, biggestAfterSmallest
          .toString))
      }
    }
  }

  private def linearIteratorToArray(i: LinearIterator): Array[Long]=  {
    val values = new Array[Long](i.estimateSize + 1)
    var valueCounter = -1
    while (!i.atEnd) {
      i.next()
      valueCounter += 1
      values(valueCounter) = i.key
    }
    values(valueCounter) = -1
    values
  }

  def intersect(values: Array[Array[Long]]): Array[Long] = {
    var intersection = values(0)
    var i = 1
    while (i < values.length) {
      intersection = intersection.intersect(values(i))
      i += 1
    }
    intersection
  }


}
