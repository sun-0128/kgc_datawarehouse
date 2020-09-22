package kgc.test

import java.io.File

import org.apache.spark.sql.SparkSession

object DataClean extends App {
  val PATH="G:\\backup\\f\\sunyong\\Java\\课件\\数仓项目\\4.基于物联网的物流数仓系统项目\\02-项目数据\\data\\volume_box.csv"
  //模板代码
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName(this.getClass.getName).getOrCreate()
  val csvFile = new File(PATH)
  spark.read.format("csv")
    .option("header",true)
    .option("delimiter",",")
    .load(csvFile.getAbsolutePath)
    .createOrReplaceTempView(csvFile.getName.split("\\.")(0))
  val sc = spark.sparkContext
  val cleanTableSql=
    s"""
      |select
      |*
      |from
      |(
      |select
      |id,task_number,leave_department_code,waybill_type,waybill_weight,waybill_volume,create_time,alter_time,handle_time,valid_status,inflow_time,waybill_number,row_number() over(distribute by id sort by inflow_time desc) rn
      |from volume_box
      |where create_time >= '2019-12-05'
      |) tmp
      |where rn=1
    """.stripMargin
  spark.sql(cleanTableSql).repartition(1).write
    .format("csv")
    .option("header",true)
    .option("delimiter",",")
    .mode("overwrite")
    .save("G:\\backup\\f\\sunyong\\Java\\课件\\数仓项目\\4.基于物联网的物流数仓系统项目\\02-项目数据\\data\\volume_box_clean.csv")
  spark.sql(cleanTableSql).repartition(1).write.saveAsTable("a")
}
