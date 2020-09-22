package cn.kgc.exp.preprocess

import java.util.Properties
import cn.kgc.exp.utils.{DateUtils, SparkMode}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * 清洗运行类
  */
object DataClean {

  val log = LoggerFactory.getLogger(this.getClass)

  private final val PATH = "path"

  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    val propPath = Thread.currentThread().getContextClassLoader
      .getResourceAsStream("resource.properties")
    properties.load(propPath)

    log.info("main method.")

    //清洗数据的批次
    val batchNo = args(0)
    //清洗配置文件名
    val cleanName = args(1)
    //获取结束时间
    val endTime = args(2) + " " + args(3)
    // true第一次清洗 false非第一次清洗
    val firstFlag = if (args(4).equals("true")) true else false
    //spark运行模式  本地/集群
    val mode = if (args(5).equals("local")) true else false
    //本地csv路径
    val path = properties.getProperty(PATH)

    log.info(s"path is : $path")

    //获取sparksession对象和模式
    val (spark, isLocal) = SparkMode.getSparkSessionMode(mode, path)

    //开始数据清洗流程
    process(spark, endTime, batchNo, cleanName, firstFlag, isLocal, path)

    spark.stop()

  }

  /**
    * 执行清洗流程
    */
  def process(spark: SparkSession, endTime: String, batchNo: String, cleanName: String, firstFlag: Boolean, isLocal: Boolean, csvPath: String) = {
    //获取配置文件
    val prop = new Properties()
    val path = Thread.currentThread().getContextClassLoader.getResourceAsStream("1h.properties")
    prop.load(path)
    //获取清洗的表清单
    val tablesClean = prop.getProperty("table.list." + batchNo)
    //将表清单转换为HashSet
    val table_hashSet = tablesClean.split(",").toSet
    //获取清洗天数
    val durationDays = prop.getProperty("duration")

    for (tableInfo <- table_hashSet) {
      var startTime: String = null
      var tableName: String = null

      log.info(s"table info : $tableInfo")

      //获取表名
      tableName = tableInfo
      //计算开始时间
      startTime = DateUtils.addDays(endTime, durationDays.toDouble * -1, "yyyy-MM-dd HH:mm:ss")
      //执行数据清洗主流程
      val hdc = new CleanProcess(tableName, startTime, endTime, cleanName, firstFlag, isLocal, csvPath)
      hdc.execute(spark)

    }
    log.info("data clean succeed!")
  }
}
