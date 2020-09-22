package cn.kgc.exp.process

import java.util.Properties
import cn.kgc.exp.utils.{RegTempOrWriteHive, SparkMode, XMLSqlParsers}
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode
import scala.collection.mutable

object ToScan {

  private final val PATH = "path"

  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    val propPath = Thread.currentThread().getContextClassLoader.getResourceAsStream("resource.properties")
    properties.load(propPath)

    //结束时间
    val endTime = args(0) + " " + args(1)

    //spark运行模式  本地/集群
    val mode = if (args(2).equals("local")) true else false
    //本地csv路径
    val path = properties.getProperty(PATH)

    //获取sparksession对象和模式
    val (spark, isLocal) = SparkMode.getSparkSessionMode(mode, path)

    val log = Logger.getLogger(this.getClass)

    val xmlParser = if (isLocal) XMLSqlParsers("ToScanLocal.xml") else XMLSqlParsers("ToScan.xml")

    val param = mutable.HashMap[String, String]("endTime" -> endTime)

    //对卸车运单进行拆包并限定部门重量及时间
    RegTempOrWriteHive.registerTempTableBySql(xmlParser, spark, "tmp1", true, "tmp1", false, param)
    //获取卸车未扫，并写入本地(集群运行可去掉)
    val res1 = RegTempOrWriteHive.registerTempTableBySql(xmlParser, spark, "res1", false, null, false, null, 1, true, "exp.unload_unscan", false, null, isLocal, "overwrite")
    // 将结果写入mysql
    if (isLocal) log.info("local mode is not nessary to save mysql") else RegTempOrWriteHive.saveASMysqlTable(res1, "unload_unscan", SaveMode.Overwrite)

    spark.stop()
  }
}

