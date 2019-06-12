package correctnessTesting

import experiments.Datasets.loadAmazonDataset
import experiments.{GraphWCOJ, WCOJ}
import org.apache.spark.sql.WCOJFunctions
import org.scalatest.BeforeAndAfterAll

class SNBGraphWCOJ extends CorrectnessTest with BeforeAndAfterAll {
  val DATASET_PATH = "/home/per/workspace/master-thesis/datasets/snb1"
  val ds = loadAmazonDataset(DATASET_PATH, sp).cache()

  override def beforeAll(): Unit = {
    WCOJFunctions.setJoinAlgorithm(GraphWCOJ)
  }

  override def afterAll(): Unit = {
    WCOJFunctions.setJoinAlgorithm(experiments.WCOJ)
  }

  "WCOJ" should behave like sparkTriangleJoins(ds)
  "WCOJ" should behave like sparkCliqueJoins(ds)
  "WCOJ" should behave like sparkCycleJoins(ds)
  "WCOJ" should behave like sparkOtherJoins(ds)
//  "WCOJ" should behave like sparkPathJoins(ds)
}
