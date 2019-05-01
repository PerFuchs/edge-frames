package experiments

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OParser
import sparkIntegration.WCOJ2WCOJExec

import scala.collection.mutable.ListBuffer

object Readers {
  implicit def algorithmRead: scopt.Read[Algorithm] = {
    scopt.Read.reads({
      case "WCOJ" => {
        WCOJ
      }
      case "bin" => {
        BinaryJoins
      }
      case _ => {
        throw new IllegalArgumentException("Algorithm can be only `WCOJ` or `bin`")
      }
    })
  }

  implicit def datasetTypeRead: scopt.Read[DatasetType] = {
    scopt.Read.reads({
      case "ama" => {
        AmazonCoPurchase
      }
      case "snb" => {
        SNB
      }
      case "liv" => {
        LiveJournal2010
      }
      case _ => {
        throw new IllegalArgumentException("Dataset type can be only `ama` or `snb`")
      }
    })
  }

  implicit def queryRead: scopt.Read[Query] = {
    scopt.Read.reads(s => {
      val queryTypes = Seq("cycle", "clique", "path")

      queryTypes.find(t => s.startsWith(t)) match {
        case Some(t) => {
          val parameter = s.replace(t, "")
          t match {
            case "cycle" => {
              val size = parameter.toInt
              Cycle(size)
            }
            case "clique" => {
              val size = parameter.toInt
              println(s"clique of size: $size")
              Clique(size)
            }
            case "path" => {
              val parts = parameter.split("|")
              PathQuery(parts(0).toInt, parts(1).toDouble)
            }
            case _ => {
              throw new IllegalArgumentException(s"Unknown query: $s")
            }
          }
        }
        case None => {
          throw new IllegalArgumentException(s"Unknown query: $s")
        }
      }
    })
  }
}

sealed trait Algorithm {
  def name: String
}

case object WCOJ extends Algorithm {
  override def name: String = "WCOJ"
}

case object BinaryJoins extends Algorithm {
  override def name: String = "Binary join"
}

sealed trait DatasetType {
  def name: String
}

case object AmazonCoPurchase extends DatasetType {
  override def name: String = "Amazon-co-purchase"
}

case object SNB extends DatasetType {
  override def name: String = "SNB"
}

case object LiveJournal2010 extends DatasetType {
  override def name: String = "LiveJournal2010"
}

sealed trait Query

case class Clique(size: Int) extends Query

case class Cycle(size: Int) extends Query

case class PathQuery(size: Int, selectivity: Double) extends Query


case class ExperimentConfig(
                             algorithms: Seq[Algorithm] = Seq(WCOJ, BinaryJoins),
                             datasetType: DatasetType = AmazonCoPurchase,
                             datasetFilePath: File = new File("."),
                             queries: Seq[Query] = Seq.empty,
                             outputPath: File = new File("."),
                             reps: Int = 1,
                             limitDataset: Int = -1
                           )


object ExperimentRunner extends App {

  val config: ExperimentConfig = parseArgs().orElse(throw new IllegalArgumentException("Couldn't parse args")).get

  println("Setting up Spark")
  val sp = setupSpark()

  val ds = loadDataset()

  runQueries()

  scala.io.StdIn.readLine("Stop?")

  sp.stop()

  private def parseArgs(): Option[ExperimentConfig] = {
    import Readers._

    val builder = OParser.builder[ExperimentConfig]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("experiment-runner"),
        head("experiment-runner", "0.1"),
        opt[Seq[Algorithm]]('a', "algorithms")
          .required()
          .action((x, c) => c.copy(algorithms = x))
          .text("The algorithm to run experiments with, `bin` or `WCOJ`"),
        opt[DatasetType]('d', "dataset-type")
          .required()
          .action((x, c) => c.copy(datasetType = x)),
        opt[File]('o', "out")
          .valueName("<measurements-output-folder>")
          .required()
          .action((x, c) => c.copy(outputPath = x)),
        opt[File]('i', "dataset-path")
          .required()
          .action((x, c) => c.copy(datasetFilePath = x)),
        opt[Seq[Query]]('q', "queries")
          .valueName("<query1>,<query2>...")
          .required()
          .action((x, c) => c.copy(queries = x)),
        opt[Int]('l', "limit")
          .optional
          .action((x, c) => c.copy(limitDataset = x)),
        opt[Int]('r', "reps")
          .optional()
          .action((x, c) => c.copy(reps = x))
      )
    }
    OParser.parse(parser1, args, ExperimentConfig())
  }

  private def setupSpark(): SparkSession = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("Spark test")
      .set("spark.executor.memory", "5g")
      .set("spark.driver.memory", "2g")
      .set("spark.sql.autoBroadcastJoinThreshold", "104857600")  // High threshold
//      .set("spark.sql.autoBroadcastJoinThreshold", "-1")  // No broadcast
//      .set("spark.sql.codegen.wholeStage", "false")
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    spark.experimental.extraStrategies = (Seq(WCOJ2WCOJExec) ++ spark.experimental.extraStrategies)
    spark
  }

  private def loadDataset(): DataFrame = {
    val dt = config.datasetType
    println(s"Loading ${dt.name} dataset from ${config.datasetFilePath}")
    var d = config.datasetType match {
      case AmazonCoPurchase => {
        Datasets.loadAmazonDataset(config.datasetFilePath.toString, sp)
      }
      case SNB => {
        Datasets.loadSNBDataset(sp, config.datasetFilePath.toString)
      }
      case LiveJournal2010 => {
        Datasets.loadLiveJournalDataset(sp, config.datasetFilePath.toString)
      }
    }
    if (config.limitDataset != -1) {
      d = d.limit(config.limitDataset)
    }
    d = d.cache()

    val count = d.count() // Trigger dataset caching
    println(s"Running on $count rows")
    d
  }

  private def runQueries() = {
    for (q <- config.queries) {
      runQuery(config.algorithms, q)
    }
  }

  private def runQuery(algorithms: Seq[Algorithm], query: Query): Unit = {
    for (algoritm <- algorithms) {
      val queryDataFrame = algoritm match {
        case BinaryJoins => {
          query match {
            case Clique(s) => {
              Queries.cliqueBinaryJoins(s, sp, ds)
            }
            case Cycle(s) => {
              Queries.cycleBinaryJoins(s, ds)
            }
            case PathQuery(s, selectivity) => {
              val (ns1, ns2) = Queries.pathQueryNodeSets(ds, selectivity)
              Queries.pathBinaryJoins(s, ds, ns1, ns2)
            }
          }
        }
        case WCOJ => {
          query match {
            case Clique(s) => {
              Queries.cliquePattern(s, ds)
            }
            case Cycle(s) => {
              Queries.cyclePattern(s, ds)
            }
            case PathQuery(s, selectivity) => {
              val (ns1, ns2) = Queries.pathQueryNodeSets(ds, selectivity)
              Queries.pathPattern(s, ds, ns1, ns2)
            }
          }
        }
      }

      val times = ListBuffer[Double]()
      for (i <- 1 to config.reps) {
        println("Starting ...") // TODO
        val start = System.nanoTime()
        val count = queryDataFrame.count()
        val end = System.nanoTime()
        val time = (end - start).toDouble / 1000000000
        println(s"Count by $algoritm $count took $time")
        times += time
      }
      println(s"Using $algoritm, $query took ${times.sum / times.size} in average over ${config.reps} repetitions.")
    }
  }


}