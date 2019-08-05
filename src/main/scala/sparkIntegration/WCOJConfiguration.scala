package sparkIntegration

import experiments.{Algorithm, WCOJAlgorithm}
import leapfrogTriejoin.MaterializingLeapfrogJoin
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import partitioning.{AllTuples, Partitioning}

import scala.collection.mutable

// TODO correct overwriting of getters and setters in Scala?
class WCOJConfiguration private(
                                 var broadcastTimeout: Int,
                                 private var parallelism: Int,
                                 private var joinAlgorithm: WCOJAlgorithm,
                                 private var partitioning: Partitioning,
                                 private var shouldMaterialize: Boolean
                               ) {

  private def this(spark: SparkSession) = {
    this(
      spark.sqlContext.getConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key).toInt,
      spark.sparkContext.getConf.get("spark.default.parallelism", "1").toInt,
      experiments.WCOJ,
      AllTuples(),
      false)
  }


  def getParallelism: Int = {
    parallelism
  }

  def setParallelism(p: Int): WCOJConfiguration = {
    parallelism = p
    println(s"Setting parallelism to $p")
    this
  }

  def setJoinAlgorithm(a: WCOJAlgorithm): WCOJConfiguration = {
    if (a == experiments.WCOJ) {
      MaterializingLeapfrogJoin.setShouldMaterialize(false)
    }
    joinAlgorithm = a
    println(s"Setting join algorithm to $a")
    this
  }

  def getJoinAlgorithm: WCOJAlgorithm = {
    joinAlgorithm
  }

  def setPartitioning(p: Partitioning): WCOJConfiguration = {
    partitioning = p
    println(s"Setting partitioning to $p")
    this
  }

  def getPartitioning: Partitioning = {
    partitioning
  }

  def setShouldMaterialize(value: Boolean): WCOJConfiguration = {
    shouldMaterialize = value
    MaterializingLeapfrogJoin.setShouldMaterialize(value)
    this
  }

  def getShouldMaterialize: Boolean = {
    shouldMaterialize
  }

  def copy: WCOJConfiguration = {
    new WCOJConfiguration(broadcastTimeout, parallelism, joinAlgorithm, partitioning, shouldMaterialize)
  }

  def from(c: WCOJConfiguration): WCOJConfiguration = {
    broadcastTimeout = c.broadcastTimeout
    setJoinAlgorithm(c.joinAlgorithm)
    setParallelism(c.parallelism)
    setPartitioning(c.partitioning)
    setShouldMaterialize(c.shouldMaterialize)
  }
}

object WCOJConfiguration {
  private[this] val configs = new mutable.HashMap[SparkContext, WCOJConfiguration]()

  def apply(spark: SparkSession): WCOJConfiguration = {
    if (configs.isDefinedAt(spark.sparkContext)) {
      throw new IllegalArgumentException(s"WCOJConfiguration for ${spark.sparkContext} already exists. Update existing configuration.")
    }

    val c = new WCOJConfiguration(spark)
    configs.put(spark.sparkContext, c)
    c
  }

  def get(sc: SparkContext): WCOJConfiguration = {
    if (!configs.isDefinedAt(sc)) {
      throw new IllegalArgumentException(s"No WCOJ configuration initialized for $sc")
    }
    configs(sc)
  }
}
