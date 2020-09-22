package cn.kgc.exp.process


import java.util.Properties
import cn.kgc.exp.utils.{RegTempOrWriteHive, SparkMode, XMLSqlParsers}
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode
import scala.collection.mutable


object Warning {

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

    val xmlParser = if (isLocal) XMLSqlParsers("WarningLocal.xml") else XMLSqlParsers("Warning.xml")

    val param = mutable.HashMap[String, String]("endTime" -> endTime)


    //查询规定时间内，指定部分外场的所有的卸车运单，拆包
    RegTempOrWriteHive.registerTempTableBySql(xmlParser, spark, "tmp1", true, "tmp1", false, null)
    //查询规定时间内，指定外场的所有装车运单，拆包
    RegTempOrWriteHive.registerTempTableBySql(xmlParser, spark, "tmp2", true, "tmp2", false, null)
    //查询规定时间内，指定部门，绑定时间为最新的所有托盘绑定叉车扫描的运单，拆包
    RegTempOrWriteHive.registerTempTableBySql(xmlParser, spark, "tmp3", true, "tmp3", false, null)
    //查询规定时间内，指定部门的所有上分拣扫描的运单并拆包剔除重复操作运单
    RegTempOrWriteHive.registerTempTableBySql(xmlParser, spark, "tmp4", true, "tmp4", false, null)
    //筛选没有装车的所有运单，判断出运单所在的位置，并写入本地(集群运行可去掉)
    val loadwarning_waybill_result = RegTempOrWriteHive.registerTempTableBySql(xmlParser, spark, "res1", false, null, false, param, 1, true, "exp.loadwarning_waybill", false, null, isLocal, "overwrite")

    // 将结果写入mysql
    if (isLocal) log.info("local mode is not nessary to save mysql") else RegTempOrWriteHive.saveASMysqlTable(loadwarning_waybill_result, "loadwarning_waybill_result", SaveMode.Overwrite)

    spark.stop()
  }
}

