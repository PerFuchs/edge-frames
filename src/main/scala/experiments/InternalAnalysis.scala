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
  val ANALYSIS = ASSERTION + 1

  val ANALYSIS_OUTPUT_FOLDER = "internal_analysis"
  val INTERSECTION_ANALYSIS_OUTPUT: String = Seq(ANALYSIS_OUTPUT_FOLDER, "intersections.csv").mkString("/")

  var intersectionAnalysisWriter: CSVWriter = new CSVWriter(new BufferedWriter(new FileWriter(INTERSECTION_ANALYSIS_OUTPUT)), ',', 0)

  @elidable(ANALYSIS)
  def setupAnalysis(config: ExperimentConfig): Unit = {
    require(!config.algorithms.contains(experiments.WCOJ), "Analysis currently only works for GraphWCOJ.")
    if (!Files.exists(Paths.get(ANALYSIS_OUTPUT_FOLDER))) {
      Files.createDirectories(Paths.get(ANALYSIS_OUTPUT_FOLDER))
    }
    intersectionAnalysisWriter.writeNext(Array("total", "smallestIterator"))
  }

  @elidable(ANALYSIS)
  def analyseIntersections(iterators: Array[TrieIterator]): Unit = {
    val materialized = iterators.map(linearIteratorToArray)
    intersectionAnalysisWriter.writeNext(Array(intersect(materialized.toList).length.toString, materialized.map(_.length).min.toString))
  }

  private def linearIteratorToArray(i: LinearIterator): Array[Long]=  {
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


}
