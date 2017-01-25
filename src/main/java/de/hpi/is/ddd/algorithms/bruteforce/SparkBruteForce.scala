package de.hpi.is.ddd.algorithms.bruteforce

import java.io.{File, Serializable}
import java.net.URI
import java.util

import de.hpi.is.ddd.evaluation.Evaluator
import de.hpi.is.idd.datasets.CoraUtility
import de.hpi.is.idd.interfaces.DatasetUtils
import org.apache.commons.lang3.tuple.Pair
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.language.implicitConversions

/**
  * Example Brute Force implementation for finding duplicates in the Apache Spark environment.
  *
  * Given the full dataset, tries to uniformly split the pair comparisons across the given nodes.
  *
  * Replicates all the data to all nodes.
  *
  */
object SparkBruteForce extends App {
  /* Number of nodes */
  var n: Int = 0

  /* Number of records */
  var m: Int = 0

  var totalComparisons: Long = 0L

  /** Paths
    *
    * For local files use: file:// + your path  (e.g.: file:///data/mydataset.csv)
    * Otherwise: hdfs:// + your path.
    */

  var dataset : String = null
  var resultDir : String = null
  var goldStandard : String = null

  /** Spark variables **/
  var sc : SparkContext = null
  var sconf : SparkConf = null
  var ses : SparkSession = null
  var sql : SQLContext = null

  /**
    *
    * Deduplicates a specific partition.
    *
    * @param partitionId: ID of the partition
    * @param assigned: The Record Indexes (offset in Seq), that are assigned to this partition.
    * @param records: Seq (equivalent of Java's util.List) of records.
    * @param du: DatasetUtils, containing everything important about a specific dataset.
    * @return : (# comparisons, mutable.Set of pairs (Record ID, Record ID))
    */
  def deduplicatePartition(partitionId: Int, assigned: Seq[Int], records: Seq[util.Map[java.lang.String, Object]],
                  du: DatasetUtils): (Long, collection.mutable.Set[(String, String)]) = {
    println(partitionId + " started")
    var cmps = 0L
    val duplicates = collection.mutable.Set[(String, String)]()
    for (i <- assigned) {
      for (j <- i+1 to records.length-1) {
        val sim: Double = du.calculateSimilarity(records.apply(i), records.apply(j), null)
        cmps += 1L
        if (sim >= du.getDatasetThreshold) {
          val pair = (records.apply(i).get("id").toString, records.apply(j).get("id").toString)
          duplicates.add(pair)
        }
      }
    }
    println(partitionId + " stopped")
    (cmps, duplicates)
  }

  /**
    *
    * Row (Scala) --> util.Map (Java)
    *
    * Convert Scala's Row to Java's util.Map
    *
    * @param row: Record in Scala's Row format.
    * @param columns: Header
    * @param du: DatasetUtil
    * @return
    */
  def rowToMap(row: Row, columns: Array[String], du: DatasetUtils): util.Map[String, Object] = {
    val recordMap = new util.HashMap[String, String]()

    for (j <- 0 to columns.length - 1) {
      recordMap.put(columns.apply(j), row.getAs(columns.apply(j)))
    }
    val recordParsed = du.parseRecord(recordMap)
    recordParsed
  }

  /**
    * Note: the distinction between Record ID and Record Index: The latter is the position of the Record in the List.
    *
    * @return : util.Set[Pair[Record ID, Record ID\]\] | Set of pairs ready to be used by the Java evaluator.
    */
  def deduplicate(du: DatasetUtils): util.Set[Pair[String, String]] = {
    /* The whole set of records, that needs to be deduplicated */
    val df = sql.read.format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .option("nullValue", "")
      .option("header", true)
      .load(dataset)
      .na.fill("")   // Fill null values with empty string ""

    /* Header */
    val columns = df.columns

    /* DataFrame --> RDD */
    val drdd = df.rdd

    /* Seq[util.Map[String, Object]] */
    val listRecordMap = drdd.map(x => rowToMap(x, columns, du)).collect().toSeq   // .cache() ??

    /* RDD[Integer: Partition ID, util.List: [Record Index]] | Integer & util.List in Java */
    val groupedPartition = sc.parallelize(ParallelBruteForce.getPartitioning(n, df.count().toInt).asScala.toSeq)//.asScala.toSeq.toMap()

    /* RDD[Int: Partition ID, Seq: [Record Index]] | Transformed in Scala */
    val groupedPartitionTransformed = groupedPartition.map(x => (x._1.toInt, x._2.asScala.toSeq.map(y => y.toInt)))

    /* RDD[mutable.Set: [(String: Record ID, String: Record ID)]] | Multiple partitions */
    val comparisonsAndDuplicatePairs = groupedPartitionTransformed.map(x =>
      deduplicatePartition(
        x._1.toInt,
        x._2,
        listRecordMap,
        du)
    )

    /* Distinguishing between the duplicate pairs, while at the same time summing all the comparisons */
    val duplicatePairs = comparisonsAndDuplicatePairs.map(x => {totalComparisons += x._1; x._2})

    /* Collapse into a single partition, transform Set into Seq |  */
    val singlePartition = duplicatePairs.flatMap(x => x.toSeq)

//    /* Alphabetically sort the results */
//    var singlePartitionSorted = singlePartition.sortBy(x => (x._1, x._2))

    /* util.Set[Pair[Record ID, Record ID]] | Transformed into Java objects */
    val javaPairs = singlePartition.collect().map(x => Pair.of(x._1, x._2)).toSet.asJava

    javaPairs  // Return javaPairs
  }

  override def main(args: Array[String]) = {
    val argsMap: util.Map[String, String] = new util.HashMap[String, String]
    for (arg <- args) {
      val toks: Array[String] = arg.split("=")
      argsMap.put(toks(0), toks(1))
    }

    val master = if (argsMap.containsKey("master")) argsMap.get("master") else "local[*]"
    dataset = if (argsMap.containsKey("dataset")) argsMap.get("dataset") else "file://" + "/data/datasets/incremental_duplicate_detection/cora/cora_v3.tsv"
    goldStandard = if (argsMap.containsKey("goldStandard")) argsMap.get("goldStandard") else  "/data/datasets/incremental_duplicate_detection/cora/cora_ground_truth.tsv"
    resultDir = if (argsMap.containsKey("resultDir")) argsMap.get("resultDir") else  "file://" + "/data/projects/project_seminar_distributed_duplicate_detection/working_dir/cora/"

    ses = SparkSession
      .builder()
      .appName("Spark DDD BruteForce")
      .master(master)
      .getOrCreate()

    sc = ses.sparkContext
    sql = ses.sqlContext
    sconf = sc.getConf

//    /* Is increasing the number of "workers", resolving the problem of the last worker? */
//    sconf.set("spark.executor.instances", "16")// 16 workers
//    sconf.set("spark.executor.cores", "1"); // 1 cores on each workers

    /* By default in a local execution, cores */
    n = sc.defaultParallelism // * 4

    val du = new CoraUtility()

    val fs = FileSystem.get(new URI(resultDir), sc.hadoopConfiguration)

    try
      fs.delete(new org.apache.hadoop.fs.Path(resultDir), true)
    catch {
      case e: Exception => {
        print(e)
      }
    }

    val evaluator: Evaluator = new Evaluator(new File(goldStandard))

    println("Duplicates (pairs)")
    val pairs = deduplicate(du)
    evaluator.setTotalComparisons(totalComparisons)

    println("Evaluation")
    val evaluation = evaluator.evaluate(pairs)

    println(evaluation)
    ses.stop()
  }
}
