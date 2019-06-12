package correctnessTesting

import experiments.Datasets._
import experiments.{GraphWCOJ, WCOJ}
import org.apache.spark.sql.WCOJFunctions
import org.scalatest.BeforeAndAfterAll

class GoogleWebWCOJ extends CorrectnessTest with BeforeAndAfterAll {
  val DATASET_PATH = "/home/per/workspace/master-thesis/datasets/web-google"
  val ds = loadGoogleWebGraph(DATASET_PATH, sp).cache()

  override def beforeAll(): Unit = {
    WCOJFunctions.setJoinAlgorithm(WCOJ)
  }

  override def afterAll(): Unit = {
    WCOJFunctions.setJoinAlgorithm(experiments.WCOJ)
  }

  "WCOJ" should behave like sparkTriangleJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkCliqueJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkCycleJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkOtherJoins(DATASET_PATH, ds)
  "WCOJ" should behave like sparkPathJoins(ds)
}
