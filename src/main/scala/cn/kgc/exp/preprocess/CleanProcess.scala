package cn.kgc.exp.preprocess

import java.text.SimpleDateFormat
import java.util.Properties
import jodd.util.StringUtil
import org.apache.commons.lang.time.FastDateFormat
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory


class CleanProcess extends Serializable {


  val log = LoggerFactory.getLogger(this.getClass)

  //local csv path
  private var path: String = _

  //配置文件中需要清洗的表名
  private var isLocal: Boolean = _

  //配置文件中需要清洗的表名
  private var cleanName: String = _

  //配置文件中需要清洗的表名
  private var cleanTable: String = _

  //需要清洗的表
  private var sourceTable: String = _

  //清洗后数据所存的表
  private var targetTable: String = _

  //查询的字段集合
  private var selectFields1: String = _
  private var selectFields2: String = _

  //条件字段集合1
  private var whereFields1: String = _

  //条件字段集合2
  private var whereFields2: String = _

  //主键
  private var primaryKey: String = _

  //主键
  private var orderBy: String = _

  //表文件个数
  private var repartitionNum: Int = _

  //基础数据判断是否第一次清洗
  private var firstFlag: Boolean = _

  //读取配置文件管理器
  private var prop: Properties = _

  var cleanTemplate: String = _

  var newTemplate: String = _

  var unionTemplate: String = _

  def this(cleanTable: String, startTime: String, endTime: String, cleanName: String, firstFlag: Boolean, isLocal: Boolean, csvPath: String) {
    this() //调用主构造器
    this.cleanTable = cleanTable
    this.cleanName = cleanName
    this.firstFlag = firstFlag
    this.isLocal = isLocal
    this.path = csvPath

    val suffix = cleanName match {
      case "1h" => "_1H"
      case "1d" => "_1D"
      case _ => log.error(s"error properties ${cleanName}!")
    }

    //获取配置文件
    prop = new Properties()
    val path = Thread.currentThread().getContextClassLoader.getResourceAsStream("SQLsource.properties")
    prop.load(path)

    if (isLocal) {
      if (prop.getProperty(s"${cleanTable}_sourceTable") == null) {
        log.error(s"error cleanTable: ${cleanTable}")
      } else {
        sourceTable = prop.getProperty(s"${cleanTable}_sourceTable").substring(4)
        targetTable = prop.getProperty(s"${cleanTable}_targetTable").substring(4) + suffix
      }
    } else {
      sourceTable = prop.getProperty(s"${cleanTable}_sourceTable")
      targetTable = prop.getProperty(s"${cleanTable}_targetTable") + suffix
    }


    selectFields1 = prop.getProperty(s"${cleanTable}_selectFields1")
    selectFields2 = prop.getProperty(s"${cleanTable}_selectFields2")
    whereFields1 = prop.getProperty(s"${cleanTable}_whereFields1")
    whereFields1 = if (whereFields1 == null) "" else whereFields1
    whereFields2 = prop.getProperty(s"${cleanTable}_whereFields2")
    whereFields2 = if (whereFields2 == null) "" else whereFields2

    val num = prop.getProperty(s"${cleanTable}_repartitionNum")
    repartitionNum = if (num == null) 5 else num.toInt
    primaryKey = prop.getProperty(s"${cleanTable}_primaryKey")
    orderBy = prop.getProperty(s"${cleanTable}_orderBy")

    setCleanTemplate(startTime, endTime, cleanName)
  }


  def setCleanTemplate(startTime: String, endTime: String, cleanName: String) = {

    // 分区取数
    val v_timeRange = FastDateFormat.getInstance("yyyyMMdd").format(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(startTime))
    val v_timeEnd = FastDateFormat.getInstance("yyyyMMdd").format(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(startTime))

    val clean_sql =
      s"""
       SELECT ${selectFields1}
        FROM
       (SELECT ${selectFields2},
           ROW_NUMBER() OVER(PARTITION BY ${primaryKey} ORDER BY ${orderBy} ) AS CLEAN_RN
        FROM ${sourceTable}
        ${whereFields1}
       ) TMP
       ${whereFields2}
     """.stripMargin

    var sql = new StringBuilder(clean_sql)
    sql = new StringBuilder(sql.replaceAllLiterally("v_startTime", startTime))
    sql = new StringBuilder(sql.replaceAllLiterally("v_endTime", endTime))
    sql = new StringBuilder(sql.replaceAllLiterally("v_timeRange", v_timeRange))
    sql = new StringBuilder(sql.replaceAllLiterally("v_timeEnd", v_timeEnd))

    cleanTemplate = sql.toString()
  }

  /**
    * 清洗前删除目标表
    *
    * @param spark
    * @param table
    */
  def deleteTable(spark: SparkSession, table: String): Unit = {

    if (StringUtil.isNotEmpty(table)) {
      if (table.toUpperCase.startsWith("EXP")) {
        //删除sql
        val sql =
          s"""
             |DROP TABLE IF EXISTS ${table}
        """.stripMargin

        log.info("delete table sql:" + sql)
        //执行
        spark.sql(sql)
        log.info("delete table" + table + "succeed!")
      } else {
        log.error("delete table name (" + table + " failed!s")
      }
    } else {
      log.warn("error sql (table name is not exist!)")
    }

  }

  /**
    * 执行sql清洗
    *
    * @param spark SparkSeesion
    */
  def execute(spark: SparkSession): Unit = {
    //注册udf
    def dateFormat(time: Any) = time match {
//      case time: java.sql.Timestamp => FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(time)
      case time:String if(time.length)>19 => time.replaceAll("T"," ").substring(0,19)
      case _ => "NULL"
    }

    spark.sqlContext.udf.register("dateFormat", dateFormat(_: Any))

    if (StringUtil.isNotEmpty(sourceTable) && StringUtil.isNotEmpty(targetTable) && StringUtil.isNotEmpty(selectFields1) && StringUtil.isNotEmpty(selectFields2)) {

      if (cleanName.equals("base")) {

        if (firstFlag) {
          //删除目标表
          deleteTable(spark, targetTable)
          // 清洗插入数据
          log.info(cleanTemplate)

          val data = spark.sql(cleanTemplate).repartition(repartitionNum)
          data.printSchema()
          val tempTable = "TMP_" + sourceTable.replace(".", "")
          data.registerTempTable(tempTable)

          val sql =
            s"""
               |CREATE TABLE ${targetTable} STORED AS ORC
               |AS
               |SELECT * FROM ${tempTable}
              """.stripMargin

          spark.sql(sql)
          spark.sqlContext.dropTempTable(tempTable)
        } else {
          // 查询前一天是否有更新数据
          log.info(newTemplate)
          val newData = spark.sql(newTemplate)

          if (newData.count() != 0) {

            val unionTable = "UNION_" + sourceTable.replace(".", "")
            val tempTable = "TMP_" + sourceTable.replace(".", "")

            // 查询历史信息数据和前一天同步数据
            log.info(unionTemplate)
            val data = spark.sql(unionTemplate)
            data.registerTempTable(unionTable)

            // 清洗合并后的数据
            var sql = new StringBuilder(cleanTemplate)
            sql = new StringBuilder(sql.replaceAllLiterally(sourceTable, unionTable))
            log.info(sql.toString())
            val cleanData = spark.sql(sql.toString()).repartition(repartitionNum)
            //建立临时表
            cleanData.registerTempTable(tempTable)

            val insertSql =
              s"""
                 |INSERT OVERWRITE TABLE ${targetTable}
                 |SELECT * FROM ${tempTable}
          """.stripMargin

            //spark.sql(insertSql)

            spark.sqlContext.dropTempTable(unionTable)
            spark.sqlContext.dropTempTable(tempTable)
          }
        }

      } else {
        //业务表清洗
        if (firstFlag) {
          //开始清洗表前删除目标表
          deleteTable(spark, targetTable)

          log.info(cleanTemplate)

          // 执行清洗sql 落地成临时表
          val data = spark.sql(cleanTemplate).repartition(repartitionNum)
          val tempTable = "TMP_" + sourceTable.replace(".", "")
          data.registerTempTable(tempTable)

          //local mode
          if (isLocal) {
            data.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(s"${path}\\${sourceTable.replace(".", "")}_clean.csv")
          } else {
            val sql =
              s"""
                 |CREATE TABLE ${targetTable} STORED AS ORC
                 |AS
                 |SELECT * FROM ${tempTable}
          """.stripMargin

            spark.sql(sql)

            spark.sqlContext.dropTempTable(tempTable)
          }
        } else {
          log.info(cleanTemplate)
          // 执行清洗sql 落地成临时表
          val data = spark.sql(cleanTemplate).repartition(repartitionNum)
          val tempTable = "TMP_" + sourceTable.replace(".", "")
          data.registerTempTable(tempTable)

          //local mode
          if (isLocal) {
            data.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(s"${path}\\${sourceTable.replace(".", "")}_clean.csv")
          } else {
            val sql =
              s"""
                 |INSERT OVERWRITE TABLE ${targetTable}
                 |SELECT * FROM ${tempTable}
          """.stripMargin

            spark.sql(sql)

            spark.sqlContext.dropTempTable(tempTable)
          }
        }
      }
      log.info("clean table" + sourceTable + "succeed!")
    } else {
      log.error("error parmeter number:+" + cleanTable + "!")
    }
  }
}
