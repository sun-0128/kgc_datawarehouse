package cn.kgc.exp.process

import java.util.Properties
import cn.kgc.exp.utils.{DateUtils, RegTempOrWriteHive, SparkMode, XMLSqlParsers}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Forecast {

  private final val PATH = "path"

  def main(args: Array[String]) {

    val properties = new Properties()
    val propPath = Thread.currentThread().getContextClassLoader.getResourceAsStream("resource.properties")
    properties.load(propPath)

    //获取参数
    val endTime = args(0) + ' ' + args(1)
    val intervalDays = args(2)
    val startTime = DateUtils.addDays(endTime, -1.0 * intervalDays.toInt, "time")
    //spark运行模式  本地/集群
    val mode = if (args(3).equals("local")) true else false
    //本地csv路径
    val path = properties.getProperty(PATH)

    //获取sparksession对象和模式
    val (spark, isLocal) = SparkMode.getSparkSessionMode(mode, path)

    //自定义UDF函数
    spark.udf.register("addMinutes", DateUtils.addMinutes(_: String, _: Int, _: String))
    spark.udf.register("minus", DateUtils.minus(_: String, _: String))

    val log = Logger.getLogger(this.getClass)

    val xmlParser = if (isLocal) XMLSqlParsers("ForecastLocal.xml") else XMLSqlParsers("Forecast.xml")

    val Forecast = new Forecast(spark, xmlParser, startTime, endTime)

    Forecast.execute(isLocal)

    // 删除历史数据,保留5天历史数据
    if (isLocal) {
      println("local mode is not necessary to delete!")
    } else {
      spark.sql(s"ALTER TABLE EXP.FORCAST_VOLUME  DROP IF EXISTS PARTITION (BATCH_NO = '${startTime}')")
    }


    spark.stop()
  }

  /**
    * 集散中心货量预测
    *
    * @param spark
    * @param xmlParser
    * @param startTime
    * @param endTime
    */
  class Forecast(spark: SparkSession,
                 xmlParser: XMLSqlParsers,
                 startTime: String,
                 endTime: String) {

    //打印参数信息
    val log = Logger.getLogger(Forecast.getClass)

    val param = mutable.HashMap[String, String]("startTime" -> startTime, "endTime" -> endTime)

    def execute(isLocal: Boolean) = {
      //缓存机构表
      RegTempOrWriteHive.registerTempTableBySql(xmlParser, spark, "tmp1", true, "tmp1", true, null)
      //获取线路基础资料
      val tmp2 = RegTempOrWriteHive.registerTempTableBySql(xmlParser, spark, "tmp2", true, "tmp2", false, null)
      tmp2.show() // 修正线路基础资料并保存到hive
      val lineDf = RegTempOrWriteHive.registerTempTableBySql(xmlParser, spark, "tmp3", true, "tmp3", true, null, 1, true, "exp.line_info", true, null, isLocal)
      //获取未签收运单
      RegTempOrWriteHive.registerTempTableBySql(xmlParser, spark, "tmp4", true, "tmp4", false)
      //运单在库存中最新记录
      RegTempOrWriteHive.registerTempTableBySql(xmlParser, spark, "tmp5", true, "tmp5", false)
      //装车任务关联获取相关时间
      RegTempOrWriteHive.registerTempTableBySql(xmlParser, spark, "tmp6", true, "tmp6", true, null, 1, false)
      //关联线路时效获取车辆时间
      RegTempOrWriteHive.registerTempTableBySql(xmlParser, spark, "tmp7", true, "tmp7", true, null, 1, false)
      //计算可出发时间接收部门运单状态
      val nextLinkDf = RegTempOrWriteHive.registerTempTableBySql(xmlParser, spark, "tmp8", true, "tmp8", true, param, 1, false)


      // 交接已出发拼接段
      val wayResult = RegTempOrWriteHive.registerTempTableBySql(xmlParser, spark, "res1", true, "res1", true, param, 60, true, "exp.forcast_volume", true, List("batch_no=REFRESH_TIME"), isLocal)

      wayResult.show()
      // 将结果写入mysql
      if (isLocal) log.info("local mode is not nessary to save mysql") else RegTempOrWriteHive.saveASMysqlTable(wayResult, "forcast_volume", SaveMode.Append)

      // 线路时效广播
      val lineInfoArray = lineDf.rdd.map(row => {
        val source_code = row.getAs[String]("leave_department_code")
        val target_code = row.getAs[String]("arrive_department_code")
        val leave_time = row.getAs[String]("leave_time")
        val run_minutes = if (row.getAs[Long]("run_minutes") == null || row.getAs[Long]("run_minutes") == "NULL") 0.toDouble else row.getAs[Long]("run_minutes").toDouble
        val unload_time = row.getAs[Long]("unload_time").doubleValue()
        ((source_code, target_code), (leave_time, run_minutes, unload_time))
      }).groupByKey().collect().toMap

      val lineInfo = spark.sparkContext.broadcast(lineInfoArray)

      // 运单走货路径转换成rdd
      val pathDf = spark.sql(xmlParser.getSqlInfo("tmp9"))

      // 运单信息与走货路径关联限定接收部门,判断出所在部门,并重新截取出未走货路径不为空
      val currentRdd = nextLinkDf.join(pathDf.select("waybill_number", "lines"), Seq("waybill_number"))
        .filter("receive_org_code is not null")
        .select(col("waybill_number"), col("bill_time"), col("goods_weight"), col("goods_volume"), col("goods_qty"), col("bill_weight"), col("bill_volume"), col("bill_qty"), col("car_number"), col("old_bill_time"), col("waybill_status"), col("customer_pickup_department_code"), col("next_day_arrive"), col("receive_org_code"), col("lines"))
        .rdd.map { case Row(waybill_number, bill_time, goods_weight, goods_volume, goods_qty, bill_weight, bill_volume, bill_qty, car_number, old_bill_time, waybill_status, customer_pickup_department_code, next_day_arrive, receive_org_code, lines) =>
        //如果车从a到b在途中算作b的运单,在途运单已由sql计算出结果
        val current = if (waybill_status == "load_noarrive") receive_org_code.toString.split("_")(1) else receive_org_code.toString.split("_")(0)
        //将运单走货路径切割
        val lineOrg: Array[String] = lines.toString.split("-")
        //算出当前部门所在的路径编码,即在整个路线里的顺序数字
        val currentIndex = lineOrg.map(_.split(":")(0)).lastIndexOf(current)
        //重新截取未走货路径
        val newLineOrg =
          if (currentIndex >= 0 && currentIndex < lineOrg.length - 1) {
            lineOrg.takeRight(lineOrg.length - currentIndex).mkString("_")
          } else
            "N"
        (waybill_number, bill_time, goods_weight, goods_volume, goods_qty, waybill_status, receive_org_code, customer_pickup_department_code, next_day_arrive, newLineOrg, bill_weight, bill_volume, bill_qty, car_number, old_bill_time)
      }

      //如果运单未走货路径大于1个,
      val resultRdd = currentRdd.filter(_._10.length > 1)
        .filter(row => (row._2 != null && row._2 != "null" && row._2 != "NULL"))
        .flatMap {
          case (waybill_number, bill_time, goods_weight, goods_volume, goods_qty, waybill_status,
          currentOrg, customer_pickup_department_code, next_day_arrive, newLine, bill_weight, bill_volume, bill_qty, car_number, old_bill_time) =>
            //获取广播变量的线路信息
            val lineInfoValue = lineInfo.value
            var handle_bill_time = bill_time.asInstanceOf[String]
            println()
            val listBuffer = ListBuffer[(String, String, String, String, String, String)]()
            var flag = 0
            //对未走货路径进行切割
            val array = newLine.split("_")
            for (i <- array.indices) {
              if ((i < array.length - 1) && flag == 0) {
                val sourceCode = array(i).split(":")(0)
                val targetCode = array(i + 1).split(":")(0)
                //线路时间
                val times = lineInfoValue.getOrElse((sourceCode, targetCode), null)
                //获取当前部门
                val pre_org = if (i == 0 && waybill_status.asInstanceOf[String].contains("load_noarrive")) currentOrg.asInstanceOf[String].split("_")(0) else if (i == 0) null else array(i - 1).split(":")(0)
                //获取路径编号
                val path_no = if (waybill_status.asInstanceOf[String].contains("load_noarrive")) (i + 2).toString else (i + 1).toString
                if (times == null) {
                  flag = 1
                } else {
                  //对线路时间进行排序
                  val sortTimes = times.toList.sortBy(_._1).map(value => {
                    val leaveTime = value._1
                    //拼接日期的可出发时间
                    val newTime = DateUtils.format(handle_bill_time, "DATE") + " " + leaveTime
                    (newTime, value._2, value._3)
                  }).filter { row => row._1 != null && row._2 != null && row._3 != null }
                  //如果可出发时间大于当日最晚时间，就取第二天的最早班次作为可出发时间,否则就取大于可出发时间最靠近的那个
                  val startTimePre = if (sortTimes.last._1 < handle_bill_time) {
                    val in = sortTimes.head
                    (DateUtils.addDays(in._1, 1.0, "TIME"), in._2, in._3)
                  }
                  else sortTimes.filter(_._1 >= handle_bill_time).head

                  val runMinutes = startTimePre._2.toInt
                  val unloadHours = startTimePre._3.toDouble

                  // 得到出发时间以及到达时间,卸车后的时间作为下一个部门的可出发时间
                  val startTime = DateUtils.format(startTimePre._1, "TIME")
                  val endTime = DateUtils.addMinutes(startTime, runMinutes, "TIME")
                  handle_bill_time = DateUtils.addHours(endTime, unloadHours, "TIME")
                  listBuffer += ((sourceCode, targetCode, startTime, endTime, path_no, pre_org))
                }
              }
            }
            listBuffer.map(v => {
              Row(waybill_number, old_bill_time.toString, car_number, bill_time.toString, goods_weight.toString, goods_volume.toString, goods_qty.toString, bill_weight.toString, bill_volume.toString,
                bill_qty.toString, waybill_status, next_day_arrive, customer_pickup_department_code, v._1, v._2, v._3.toString, v._4.toString, v._5, v._6)
            })
        }

      val schema = StructType(
        List(
          StructField("WAYBILL_NUMBER", DataTypes.StringType), //运单号
          StructField("OLD_BILL_TIME", DataTypes.StringType), //原开单时间
          StructField("CAR_NUMBER", DataTypes.StringType), //车牌号
          StructField("BILL_TIME", DataTypes.StringType), //开单时间
          StructField("GOODS_WEIGHT", DataTypes.StringType, true), //重量
          StructField("GOODS_VOLUME", DataTypes.StringType, true), //体积
          StructField("GOODS_QTY", DataTypes.StringType, true), //件数
          StructField("BILL_WEIGHT", DataTypes.StringType, true), //开单重量
          StructField("BILL_VOLUME", DataTypes.StringType, true), //开单体积
          StructField("BILL_QTY", DataTypes.StringType, true), //开单件数
          StructField("WAYBILL_STATUS", DataTypes.StringType), //运单状态
          StructField("NEXT_DAY_ARRIVE", DataTypes.StringType), //次日达
          StructField("CUSTOMER_PICKUP_DEPARTMENT_CODE", DataTypes.StringType), //提货网点编码
          StructField("DEPART_ORG_CODE", DataTypes.StringType), //出发部门
          StructField("ARRIVE_ORG_CODE", DataTypes.StringType), //到达部门
          StructField("START_TIME", DataTypes.StringType), //预计出发时间
          StructField("END_TIME", DataTypes.StringType), //预计到达时间
          StructField("PRE_ORG_CODE", DataTypes.StringType), //上一部门名称
          StructField("PATH_NO", DataTypes.StringType) //路段号
        ))
      val dataFrame = spark.createDataFrame(resultRdd, schema)
      dataFrame.registerTempTable("tm_t_not_way")

      // 非在途运单预计出发时间、预计到达时间写hive
      val notwayResult = RegTempOrWriteHive.registerTempTableBySql(xmlParser, spark, "res2", false, null, false, param, 60, true, "exp.forcast_volume", false, List("batch_no=REFRESH_TIME"), isLocal, "append")
      // 将结果写入mysql
      if (isLocal) log.info("local mode is not nessary to save mysql") else RegTempOrWriteHive.saveASMysqlTable(notwayResult, "forcast_volume", SaveMode.Append)


    }
  }

}

