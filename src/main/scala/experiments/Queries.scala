package experiments

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import sparkIntegration.implicits._

object Queries {
  val FIXED_SEED_1 = 42
  val FIXED_SEED_2 = 220

  def pathQueryNodeSets(rel: DataFrame, selectivity: Double = 0.1): (DataFrame, DataFrame) = {
    (rel.selectExpr("src AS a").sample(selectivity, FIXED_SEED_1),
      rel.selectExpr("dst AS z").sample(selectivity, FIXED_SEED_2))
  }

  /**
    *
    * @param df
    * @param cols
    * @return `df` with filters such that all `cols` hold distinct values per row.
    */
  def withDistinctColumns(df: DataFrame, cols: Seq[String]): DataFrame = {
    var r = df
    for (c <- cols.combinations(2)) {
      r = r.where(new Column(c(0)) =!= new Column(c(1)))
    }
    r
  }

  def withSmallerThanColumns(df: DataFrame, cols: Seq[String]): DataFrame = {
    var r = df
    for (c <- cols.combinations(2)) {
      require(c(0) < c(1))
      r = r.where(new Column(c(0)) < new Column(c(1)))
    }
    r
  }

  def pathBinaryJoins(size: Int, ds: DataFrame, ns1: DataFrame, ns2: DataFrame): DataFrame = {
    size match {
      case 2 => {
        twoPathBinaryJoins(ds, ns1, ns2)
      }
      case 3 => {
        threePathBinaryJoins(ds, ns1, ns2)
      }
      case 4 => {
        fourPathBinaryJoins(ds, ns1, ns2)
      }
    }
  }

  def pathPattern(size: Int, ds: DataFrame, ns1: DataFrame, ns2: DataFrame): DataFrame = {
    val leftRel = ds.join(ns1.selectExpr("a AS src"), Seq("src"), "left_semi")
    val rightRel = ds.join(ns2.selectExpr("z AS dst"), Seq("dst"), "left_semi")
      .select("src", "dst") // Necessary because Spark reorders the columns

    val verticeNames = ('a' to 'z').toList.map(c => s"$c").slice(0, size) ++ Seq("z")

    val pattern = verticeNames.zipWithIndex.filter({ case (_, i) => {
      i < verticeNames.size - 1
    }
    }).map({ case (v1, i) => {
      s"($v1) - [] -> (${verticeNames(i + 1)})"
    }
    }).mkString(";")

    val variableOrdering = size match {
      case 2 => {
        Seq("a", "b", "z")
      }
      case 3 => {
        Seq("a", "b", "c", "z")  // takes 0.285 against 3.975 for a, b, z, c on the first 500000 edges of the directed twitter set
      }
      case 4 => {
        Seq("a", "b", "c", "d", "z")
      }
    }

    val relationships = Seq(leftRel) ++ (0 until size - 2).map(i => ds.alias(s"edges$i")) ++ Seq(rightRel)
    ds.findPattern(pattern, variableOrdering, distinctFilter = true, smallerThanFilter = false, relationships).selectExpr(verticeNames: _*)
  }

  private def twoPathBinaryJoins(rel: DataFrame, nodeSet1: DataFrame, nodeSet2: DataFrame): DataFrame = {
    val relLeft = rel.selectExpr("src AS a", "dst AS b").join(nodeSet1, Seq("a"), "left_semi")
    val relRight = rel.selectExpr("dst AS z", "src AS b").join(nodeSet2, Seq("z"), "left_semi")

    withDistinctColumns(relLeft.join(relRight, Seq("b")).selectExpr("a", "b", "z"), Seq("a", "b", "z"))
  }

  private def threePathBinaryJoins(rel: DataFrame, nodeSet1: DataFrame, nodeSet2: DataFrame): DataFrame = {
    val relLeft = rel.selectExpr("src AS a", "dst AS b").join(nodeSet1, Seq("a"), "left_semi")
    val relRight = rel.selectExpr("dst AS z", "src AS c").join(nodeSet2, Seq("z"), "left_semi")

    val middleLeft = relLeft.join(rel.selectExpr("src AS b", "dst AS c"), Seq("b")).selectExpr("a", "b", "c")
    withDistinctColumns(relRight.join(middleLeft, "c").select("a", "b", "c", "z"), Seq("a", "b", "c", "z"))
  }

  private def fourPathBinaryJoins(rel: DataFrame, nodeSet1: DataFrame, nodeSet2: DataFrame): DataFrame = {
    val relLeft = rel.selectExpr("src AS a", "dst AS b").join(nodeSet1, Seq("a"), "left_semi")
    val relRight = rel.selectExpr("dst AS z", "src AS d").join(nodeSet2, Seq("z"), "left_semi")

    val middleLeft = relLeft.join(rel.selectExpr("src AS b", "dst AS c"), Seq("b")).selectExpr("a", "b", "c")
    val middleRight = relRight.join(rel.selectExpr("src AS c", "dst AS d"), Seq("d")).selectExpr("c", "d", "z")
    withDistinctColumns(middleRight.join(middleLeft, "c").select("a", "b", "c", "d", "z"), Seq("a", "b", "c", "d", "z"))
  }

  def cliqueBinaryJoins(size: Int, sp: SparkSession, ds: DataFrame): DataFrame = {
    size match {
      case 3 => {
        withSmallerThanColumns(triangleBinaryJoins(sp, ds), Seq("a", "b", "c"))
      }
      case 4 => {
        withSmallerThanColumns(fourCliqueBinaryJoins(sp, ds), Seq("a", "b", "c", "d"))
      }
      case 5 => {
        withSmallerThanColumns(fiveCliqueBinaryJoins(sp, ds), Seq("a", "b", "c", "d", "e"))
      }
      case 6 => {
        withSmallerThanColumns(sixCliqueBinaryJoins(sp, ds), Seq("a", "b", "c", "d", "e", "f"))
      }
    }
  }

  private def triangleBinaryJoins(spark: SparkSession, rel: DataFrame): DataFrame = {
    import spark.implicits._

    val duos = rel.as("R")
      .joinWith(rel.as("S"), $"R.dst" === $"S.src")
    val triangles = duos.joinWith(rel.as("T"),
      condition = $"_2.dst" === $"T.dst" && $"_1.src" === $"T.src")

    triangles.selectExpr("_2.src AS a", "_1._1.dst AS b", "_2.dst AS c")
  }


  // TODO run binaries on worst order
  private def fourCliqueBinaryJoins(sp: SparkSession, rel: DataFrame): DataFrame = {
    import sp.implicits._
    val triangles = triangleBinaryJoins(sp, rel)

    val fourClique =
      triangles.join(rel.alias("ad"), $"src" === $"a")
        .selectExpr("a", "b", "c", "dst AS d")
        .join(rel.alias("bd"), $"src" === $"b" && $"dst" === $"d", "left_semi")
        .join(rel.alias("cd"), $"src" === $"c" && $"dst" === $"d", "left_semi")
    fourClique.select("a", "b", "c", "d")
  }


  private def fiveCliqueBinaryJoins(sp: SparkSession, rel: DataFrame): DataFrame = {
    import sp.implicits._


    fourCliqueBinaryJoins(sp, rel)
      .join(rel.alias("ae"), $"src" === $"a")
      .selectExpr("a", "b", "c", "d", "dst AS e")
      .join(rel.alias("be"), $"src" === $"b" && $"dst" === $"e", "left_semi")
      .join(rel.alias("ce"), $"src" === $"c" && $"dst" === $"e", "left_semi")
      .join(rel.alias("de"), $"src" === $"d" && $"dst" === $"e", "left_semi")
      .select("a", "b", "c", "d", "e")
  }

  private def sixCliqueBinaryJoins(sp: SparkSession, rel: DataFrame): DataFrame = {
    import sp.implicits._


    fiveCliqueBinaryJoins(sp, rel)
      .join(rel.alias("af"), $"src" === $"a")
      .selectExpr("a", "b", "c", "d", "e", "dst AS f")
      .join(rel.alias("bf"), $"src" === $"b" && $"dst" === $"f", "left_semi")
      .join(rel.alias("cf"), $"src" === $"c" && $"dst" === $"f", "left_semi")
      .join(rel.alias("df"), $"src" === $"d" && $"dst" === $"f", "left_semi")
      .join(rel.alias("ef"), $"src" === $"e" && $"dst" === $"f", "left_semi")
      .select("a", "b", "c", "d", "e", "f")
  }

  val fiveVerticePermuations = ('a' to 'e').map(c => s"$c").permutations.toList
  var permCounter = 0

  def cliquePattern(size: Int, rel: DataFrame, useDistinctFilter: Boolean = false): DataFrame = {
    //    val perm = fiveVerticePermuations(permCounter)
    //    permCounter += 1
    val alphabet = 'a' to 'z'
    val verticeNames = alphabet.slice(0, size).map(_.toString)

    val pattern = verticeNames.combinations(2).filter(e => e(0) < e(1))
      .map(e => s"(${e(0)}) - [] -> (${e(1)})")
      .mkString(";")
    //    println(s"Perm: ${perm.mkString(",")} at position $permCounter")
    rel.findPattern(pattern, verticeNames, distinctFilter = useDistinctFilter, smallerThanFilter = !useDistinctFilter)
  }

  def diamondPattern(rel: DataFrame): DataFrame = {
    withDistinctColumns(rel.findPattern(
      """
        |(a) - [] -> (b);
        |(b) - [] -> (c);
        |(b) - [] -> (c);
        |(c) - [] -> (d);
        |(d) - [] -> (a)
        |""".stripMargin, List("a", "b", "c", "d")), List("a", "b", "c", "d"))
  }

  def diamondBinaryJoins(rel: DataFrame): DataFrame = {
    withDistinctColumns(rel.selectExpr("src AS a", "dst AS b")
      .join(rel.selectExpr("src AS b", "dst AS c"), "b")
      .join(rel.selectExpr("src AS c", "dst AS d"), "c")
      .join(rel.selectExpr("src AS d", "dst AS a"), Seq("a", "d"), "left_semi"), Seq("a", "b", "c", "d"))
  }

  def houseBinaryJoins(sp: SparkSession, rel: DataFrame): DataFrame = {
    import sp.implicits._

    withDistinctColumns(fourCliqueBinaryJoins(sp, rel)
      .join(rel.alias("ce"), $"src" === $"c")
      .selectExpr("a", "b", "c", "d", "dst AS e")
      .join(rel.alias("de"), $"src" === $"d" && $"dst" === $"e", "left_semi")
      .select("a", "b", "c", "d", "e"),
      Seq("a", "b", "c", "d", "e"))
  }

  def housePattern(rel: DataFrame): DataFrame = {
    rel.findPattern(
      """
        |(a) - [] -> (b);
        |(b) - [] -> (c);
        |(c) - [] -> (d);
        |(a) - [] -> (d);
        |(a) - [] -> (c);
        |(b) - [] -> (d);
        |(c) - [] -> (e);
        |(d) - [] -> (e)
        |""".stripMargin, List("a", "b", "c", "d", "e"), distinctFilter = true)
  }

  def kiteBinary(sp: SparkSession, rel: DataFrame): DataFrame = {
    import sp.implicits._

    triangleBinaryJoins(sp, rel)
      .join(rel.alias("cd"), $"src" === $"c")
      .selectExpr("a", "b", "c", "dst AS d")
      .join(rel.alias("bd"), $"src" === $"b" && $"dst" === $"d", "left_semi")
      .filter($"c" < $"b" && $"b" < $"d" && $"d" < $"a" )
      .selectExpr("a", "b", "c", "d")
  }

  def kitePattern(rel: DataFrame): DataFrame = {
    rel.findPattern(
      """
        |(a) - [] -> (b);
        |(a) - [] -> (c);
        |(b) - [] -> (c);
        |(b) - [] -> (d);
        |(c) - [] -> (d)
        |""".stripMargin, List("c", "b", "d", "a"), smallerThanFilter = true)
      .selectExpr("a", "b", "c", "d")
  }

  def cycleBinaryJoins(size: Int, rel: DataFrame): DataFrame = {
    require(3 < size, "Use this method only for cyclics of four nodes and above")

    val verticeNames = ('a' to 'z').toList.map(c => s"$c")

    var ret = rel.selectExpr("src AS a", "dst AS b")
    for (i <- 1 until (size - 1)) {
      ret = ret.join(rel.selectExpr(s"src AS ${verticeNames(i)}", s"dst AS ${verticeNames(i + 1)}"), verticeNames(i))
    }
    ret = ret.join(rel.selectExpr(s"src AS ${verticeNames(size - 1)}", s"dst AS a"), Seq("a", verticeNames(size - 1)), "left_semi")
      .select(verticeNames.head, verticeNames.slice(1, size): _*)
    withDistinctColumns(ret, verticeNames.slice(0, size))
  }

  def cyclePattern(size: Int, rel: DataFrame): DataFrame = {
    require(3 < size, "Use this method only for cyclics of four nodes and above")

    val verticeNames = ('a' to 'z').toList.map(c => s"$c").slice(0, size)

    val pattern = verticeNames.zipWithIndex.map({ case (v1, i) => {
      s"($v1) - [] -> (${verticeNames((i + 1) % size)})"
    }
    }).mkString(";")


    val variableOrdering = size match {
      case 4 => {
        Seq("a", "b", "c", "d")
      }
      case 5 => {
        Seq("a", "b", "e", "c", "d")
      }
      case 6 => {
        Seq("a", "b", "f", "c", "e", "d")
      }
    }

    rel.findPattern(pattern, variableOrdering, distinctFilter = true)
  }

}
