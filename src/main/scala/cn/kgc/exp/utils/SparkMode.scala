package cn.kgc.exp.utils

import java.io.File

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * 获取spark对象和运行模式
  */
object SparkMode {

  val log = LoggerFactory.getLogger(this.getClass)

  def getSparkSessionMode(mode: Boolean, path: String): (SparkSession, Boolean) = {

    val (spark, isLocal) = if (mode) {
      //设置hadoop-home
      // System.setProperty("hadoop.home.dir", "E:\\2018\\hadoop-home\\hadoop-common-2.2.0-bin-master\\hadoop-common-2.2.0-bin-master")

      val spark = SparkSession.builder()
        .master("local[1]")
        .appName("expLocal")
        .getOrCreate()

      //读取csv注册临时表
      val csvFiles = new File(path)
      if (csvFiles.isDirectory) {
        csvFiles.listFiles().foreach { sub =>
          spark.read.format("csv")
            .option("delimiter", ",")
            .option("header", "true")
            .option("quote", "'")
            .option("nullValue", "\\N")
            .option("inferSchema", "true").load(sub.getAbsolutePath).registerTempTable(s"${sub.getName.split("\\.")(0)}")
        }
      }
      (spark, true)
    } else {
      (SparkSession.builder()
        .appName("exp")
        .enableHiveSupport()
        .getOrCreate(), false)
    }
    (spark, isLocal)
  }
}
