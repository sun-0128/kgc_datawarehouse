package cn.kgc.exp.utils

import scala.collection.mutable
import scala.util.parsing.combinator.RegexParsers
import scala.xml.XML

/**
  * 解析sql配置文件
  * @param filename 文件名
  */
class XMLSqlParsers(filename: String) extends RegexParsers {

  override val whiteSpace = "".r

  /**
    * 该方法用于从配置文件中读取sql信息
    *
    * @param source
    * @tparam U
    * @return
    */
  def getSqlInfo[U](source: U): String = {
    val is = Thread.currentThread().getContextClassLoader.getResourceAsStream("xml/" + filename)
    val xmlFile = XML.load(is)

    //获取所有"select"节点
    val selects = xmlFile \ "select"
    var sql = ""

    //获取指定编号的sql片段
    for (s <- selects if (s \ "@id").text == source)
      sql = s.child.text

    if (sql != null && sql != "") sql = sql.trim

    sql
  }

  /**
    * 解析sql文本并替换其中出现的变量
    *
    * @param sqlId  sql编号
    * @param params 参数集合(参数名1->参数值1,参数名2->参数值2)
    * @return 返回解析后的sql
    */
  def parserSql(sqlId: String, params: mutable.HashMap[String, String] = null): String = {
    val sql = getSqlInfo(sqlId)

    //定义正则表达式匹配的模式
    val words = "([\\d\\w\\s,<>=!*.+|'()_:-]|[%/&]|[（）]|[、]|[\\u4E00-\\u9FFF]|[\"])+".r

    def expr: Parser[String] = factor ~ opt(expr) ^^ {
      case t ~ Some(e) => t + e
      case t ~ None => t
    }

    def factor: Parser[String] = words ^^ { x => x } | "{" ~ expr ~ "}" ^^ {
      case "{" ~ t ~ "}" => if (params != null) params.getOrElse(t, "") else "''"
    }

    val result = this.parseAll(expr, sql)

    if (result.successful) result.get
    else "Parser sql from xml file Fail." + result.get
  }
}

object XMLSqlParsers {

  def apply(filename: String): XMLSqlParsers = new XMLSqlParsers(filename)
}




