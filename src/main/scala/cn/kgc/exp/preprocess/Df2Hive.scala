package cn.kgc.exp.preprocess

import java.util.ArrayList
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * dataframe to  hive
  * include create insert override
  */
object Df2Hive {

  Logger.getLogger("org.apache.spark").setLevel(Level.INFO)

  val log = Logger.getLogger(this.getClass)

  /**
    * 通过自定义分区个数动态分区  是否要覆盖原始数据
    *
    * @param df              DataFrame
    * @param tableName       插入的表名称
    * @param partitionFields 分区字段
    * @param isOverride      是否要覆盖
    * @param repartitionNum  分区格式
    * @param spark    hive上下文
    */
  def hiveDynamicPartitionsNumWithIsOverrideInsert(df: DataFrame, tableName: String, partitionFields: List[String], isOverride: Boolean, repartitionNum: Int, spark: SparkSession): Unit = {

    val temTableName = tableName.replace(".", "_") + "_df";

    log.info("dynamicInsertToHivePartition start!")

    //开启动态分区类
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    //无限制模式
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    df.registerTempTable(temTableName)

    //获取方法属性类
    val DFToHiveTable = new DFToHiveTable(df, temTableName, tableName, partitionFields, true, isOverride);

    DFToHiveTable()

    //获取创建sql
    var createSql = DFToHiveTable.getCreateTableSql();

    if (createSql.length > 0) {
      //执行创表语句
      spark.sql(createSql.toString());
      log.info("createSql:" + createSql.toString())
    }

    //获取创建sql
    var insertSql = DFToHiveTable.getInsertTableSql();

    if (insertSql.length > 0) {
      //执行创表语句
      spark.sql(insertSql.toString());
      log.info("dynamic insert table sql:" + insertSql.toString())
    }
  }

  /**
    * 分区字段全部以dt='20200202’, dt2='20200203’ 形式传入
    */
  private class DFToHiveTable(dataFrame: DataFrame, temTableName: String, tableName: String, partitionFields: List[String], isDynamic: Boolean, isOverride: Boolean) {
    //字段列
    var field_col = new StringBuffer("")
    //字段列带有类型
    var field_col_withType = new StringBuffer("")

    //获取分区列字段 创建表时候用
    var createPartitionCols = new StringBuffer("")
    //获取分区列字段有类型
    var insertPartitionCols_withValue = new StringBuffer("")

    //获取分区列字段有类型
    var selectPartitionCols_withValue = new StringBuffer("")

    //分割字段
    val fieldSpliteField = "="

    //存储分区字段
    var sPartitionField = new ArrayList[String]();

    //存储分区字段
    val sPartitionColField = new StringBuffer("")

    /**
      * 获取列信息
      */
    private def getColumnInfo(dataFrame: DataFrame): Unit = {

      //获取字段，值
      val arr = dataFrame.schema.fieldNames

      //
      if (partitionFields != null) {
        partitionFields.foreach(col => {

          if (col.contains(";")) {
            //对，号进行特殊处理
            val sCols = col.split(";")
            sCols.foreach(scos => {

              if (scos.contains(fieldSpliteField)) {
                val splitFiled = scos.substring(0, scos.indexOf(fieldSpliteField)).trim.toLowerCase;
                val splitFiledValue = scos.substring(scos.indexOf(fieldSpliteField) + fieldSpliteField.length).trim;
                createPartitionCols.append(splitFiled).append(" string").append(",") //特殊处理

                if (isDynamic) {
                  selectPartitionCols_withValue.append(splitFiledValue.toLowerCase).append(",")
                  insertPartitionCols_withValue.append(splitFiled).append(",")
                } else {
                  insertPartitionCols_withValue.append(scos).append(",")
                }

                sPartitionField.add(splitFiled)
              }
            })

          } else {
            if (col.contains(fieldSpliteField)) {
              val splitFiled = col.substring(0, col.indexOf(fieldSpliteField)).trim.toLowerCase;
              val splitFiledValue = col.substring(col.indexOf(fieldSpliteField) + fieldSpliteField.length).trim;
              createPartitionCols.append(splitFiled).append(" string").append(",") //特殊处理

              if (isDynamic) {
                selectPartitionCols_withValue.append(splitFiledValue.toLowerCase).append(",")
                insertPartitionCols_withValue.append(splitFiled).append(",")
              } else {
                insertPartitionCols_withValue.append(col).append(",")
              }
              sPartitionField.add(splitFiled)
            }
          }

        })
      }

      //循环遍历列
      arr.foreach(arrField => {

        //全部转小写
        var field = arrField.toLowerCase;

        if (sPartitionField != null && sPartitionField.size() > 0) {
          if (sPartitionField.contains(field)) {
            sPartitionColField.append(field + ",")
          } else {
            field_col.append(field + ",")
            field_col_withType.append(field + " string" + ",")
          }
        } else {
          field_col.append(field + ",")
          field_col_withType.append(field + " string" + ",")
        }

      })

      //去除最后的,
      if (field_col.length() > 0) {
        field_col = field_col.deleteCharAt(field_col.length - 1)
        field_col_withType = field_col_withType.deleteCharAt(field_col_withType.length - 1)
      }

      //去除最后的,
      if (createPartitionCols.length() > 0) {
        createPartitionCols = createPartitionCols.deleteCharAt(createPartitionCols.length - 1)
        insertPartitionCols_withValue = insertPartitionCols_withValue.deleteCharAt(insertPartitionCols_withValue.length - 1)
      }

      if (selectPartitionCols_withValue.length() > 0) {
        selectPartitionCols_withValue = selectPartitionCols_withValue.deleteCharAt(selectPartitionCols_withValue.length - 1)
      }
    }

    /**
      * 获取创建table sql
      *
      * @return
      */
    def getCreateTableSql(): String = {

      //拼接创建语句
      val createSql = new StringBuilder(" create table IF NOT EXISTS ")
        .append(tableName) //table名称
        .append("(").append(field_col_withType.toString).append(")")
      if (createPartitionCols.toString.length() > 0) {
        createSql.append(" partitioned by (" + createPartitionCols.toString + ") ")
      }
      createSql.toString();
    }


    /**
      * 获取插入table sql
      *
      * @return
      */
    def getInsertTableSql(): String = {

      //插入语句
      var insertSql = new StringBuilder()
      if (isOverride) {
        insertSql.append(" insert overwrite table ")
      } else {
        insertSql.append(" insert into table ")
      }
      //插入语句
      insertSql.append(tableName) //table名称
      if (insertPartitionCols_withValue.toString.length > 0) {
        insertSql.append(" partition (").append(insertPartitionCols_withValue.toString).append(" ) ") //分区字段
      }

      insertSql.append(" select ").append(field_col.toString)

      if (selectPartitionCols_withValue.toString.length > 0) {
        insertSql.append(",").append(selectPartitionCols_withValue.toString)
      }

      insertSql.append(" from ").append(temTableName) //原表插入语句

      insertSql.toString();
    }

    //定义方法入口
    def apply() = {

      getColumnInfo(dataFrame)
    }
  }

}