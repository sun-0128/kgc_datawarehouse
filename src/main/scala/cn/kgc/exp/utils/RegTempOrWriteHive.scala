package cn.kgc.exp.utils

import java.io.FileInputStream
import java.util.Properties
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import cn.kgc.exp.preprocess.Df2Hive
import java.sql.{Connection, DriverManager}
import scala.collection.mutable
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object RegTempOrWriteHive {

  val log = LoggerFactory.getLogger(this.getClass)

  private final val JDBC_DRIVER = "jdbc.driver"
  private final val JDBC_URL = "jdbc.url"
  private final val JDBC_USER = "jdbc.user"
  private final val JDBC_PASSWORD = "jdbc.password"

  private final val PATH = "path"

  val properties = new Properties()
  val path = Thread.currentThread().getContextClassLoader.getResourceAsStream("resource.properties")
  properties.load(path)

  // 转成string
  val stringify = udf { (vs: Any) =>
    vs match {
      case null => "null"
      case _ => vs.toString
    }
  }

  /**
    * 将DataFrame保存为Mysql表
    *
    * @param df 需要保存的dataFrame
    * @param tableName 保存的mysql 表名
    * @param saveMode  保存的模式 ：Append、Overwrite、ErrorIfExists、Ignore
    */
  def saveASMysqlTable(df: DataFrame, tableName: String, saveMode: SaveMode) = {

    var table = tableName
    val prop = new Properties //配置文件中的key 与 spark 中的 key 不同 所以 创建prop 按照spark 的格式 进行配置数据库
    prop.setProperty("user", properties.getProperty(JDBC_USER))
    prop.setProperty("password", properties.getProperty(JDBC_PASSWORD))
    prop.setProperty("driver", properties.getProperty(JDBC_DRIVER))
    prop.setProperty("url", properties.getProperty(JDBC_URL))
    if (saveMode == SaveMode.Overwrite) {
      var conn: Connection = null
      try {
        conn = DriverManager.getConnection(
          prop.getProperty("url"),
          prop.getProperty("user"),
          prop.getProperty("password")
        )
        val stmt = conn.createStatement
        table = table.toUpperCase
        stmt.execute(s"truncate table $table") //为了不删除表结构，先truncate 再Append
        conn.close()
      }
      catch {
        case e: Exception => log.error(s"MySQL Error: $e")
      }
    }
    val result = df.columns.foldLeft(df)((df, colum) => df.withColumn(colum, stringify(col(colum))))
    result.write.mode(SaveMode.Append).jdbc(prop.getProperty("url"), table.toLowerCase(), prop)
  }

  /**
    * 注册临时表或写入hive
    */
  def registerTempTableBySql(xmlParser: XMLSqlParsers,
                             spark: SparkSession,
                             sqlId: String,
                             toTempTable: Boolean = true,
                             tempTableName: String,
                             cacheFlag: Boolean = false,
                             parameter: mutable.HashMap[String, String] = null,
                             repartitionNum: Int = 10,
                             toHiveTable: Boolean = false,
                             hiveTableName: String = "",
                             overwrite: Boolean = false,
                             partitionField: List[String] = List.empty,
                             isLocal: Boolean = false,
                             localMode: String = "overwrite"): DataFrame = {

    val df = spark.sql(xmlParser.parserSql(sqlId, parameter))

    if (toTempTable) df.repartition(repartitionNum).registerTempTable(tempTableName)

    if (cacheFlag) spark.sqlContext.cacheTable(tempTableName)

    if (isLocal && toHiveTable) {
      //处理csv为null为String
      val csvResult = df.columns.foldLeft(df)((df, colum) => df.withColumn(colum, stringify(col(colum))))

      csvResult.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").mode(localMode).save(s"${properties.getProperty(PATH)}\\${hiveTableName.substring(4)}_result.csv")
    }
    else if (toHiveTable) Df2Hive.hiveDynamicPartitionsNumWithIsOverrideInsert(df, hiveTableName, partitionField, overwrite, repartitionNum, spark)

    df
  }

}
