package cn.kgc.exp.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import org.apache.commons.lang.time.FastDateFormat
import org.slf4j.LoggerFactory


/**
  * 日期时间工具类
  */
object DateUtils {

  val log = LoggerFactory.getLogger(this.getClass)

  val TIME_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TIME_FORMAT2 = FastDateFormat.getInstance("yyyy-MM-ddHH:mm:ss")
  val DATE_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd")
  val MONTH_FORMAT = FastDateFormat.getInstance("yyyy-MM")
  val DATETIME_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd 00:00:00")
  val HHMMSS_FORMAT = FastDateFormat.getInstance("HH:mm:ss")
  val TIMEASKEY_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")
  val MINSASKEY_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmm")
  val HOURSASKEY_FORMAT = FastDateFormat.getInstance("yyyyMMddHH")
  val DATEASKEY_FORMAT = FastDateFormat.getInstance("yyyyMMdd")
  val MONTHASKEY_FORMAT = FastDateFormat.getInstance("yyyyMM")
  //  新增到分钟的格式
  val YYMMDDHHMM_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm")
  val YYMMDDHH_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH")
  val YYMMDDHH0000_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:00:00")

  val TIME_REGEX = """\d{4}-\d{1,2}-\d{1,2}\s+\d{1,2}:\d{1,2}:\d{1,2}[.]?\d*""".r
  val TIME_REGEX2 = """\d{4}-\d{1,2}-\d{1,2}\d{1,2}:\d{1,2}:\d{1,2}""".r
  val TIME_REGEX3 = """\d{4}-\d{1,2}-\d{1,2}\s+\d{1,2}:\d{1,2}""".r
  val TIME_REGEX4 = """\d{4}/\d{1,2}/\d{1,2}\s+\d{1,2}:\d{1,2}:\d{1,2}""".r
  val TIME_REGEX5 = """\d{4}/\d{1,2}/\d{1,2}\s+\d{1,2}:\d{1,2}""".r
  val DATE_REGEX = """\d{4}-\d{1,2}-\d{1,2}""".r
  val TIMEASKEY_REGEX = """\d{4}\d{2}\d{2}\d{2}\d{2}\d{2}""".r
  val DATEASKEY_REGEX = """\d{4}\d{2}\d{2}""".r
  val YYYYMM_REGEX = """\d{4}\d{2}""".r
  val HHMM_REGEX = """\d{1,2}:\d{1,2}""".r //新增处理hh:mi格式的正则表达式
  val HHMMSS_REGEX =
    """\d{1,2}:\d{1,2}:\d{1,2}[.]?\d*""".r //新增处理hh:mi:ss格式的正则表达式
  /**
    * 将字符串转换为时间
    *
    * @param text 源字符串
    * @return
    */
  def parse(text: String): Date = {
    if (text != null && !"".equals(text)) {
      val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val TIME_FORMAT2 = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss")
      val TIME_FORMAT3 = new SimpleDateFormat("yyyy-MM-dd HH:mm")
      val TIME_FORMAT4 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
      val TIME_FORMAT5 = new SimpleDateFormat("yyyy/MM/dd HH:mm")
      val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
      val HHMMSS_FORMAT = new SimpleDateFormat("HH:mm:ss")
      val HHMMSS_FORMAT2 = new SimpleDateFormat("HH:mm")
      val TIMEASKEY_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss")
      val DATEASKEY_FORMAT = new SimpleDateFormat("yyyyMMdd")
      val YYYYMM_FORMAT = new SimpleDateFormat("yyyyMM")
      text match {
        case TIME_REGEX(_*) => TIME_FORMAT.parse(text)
        case TIME_REGEX2(_*) => TIME_FORMAT2.parse(text)
        case TIME_REGEX3(_*) => TIME_FORMAT3.parse(text) //新增,处理yyyy-MM-dd HH:mm格式
        case TIME_REGEX4(_*) => TIME_FORMAT4.parse(text)
        case TIME_REGEX5(_*) => TIME_FORMAT5.parse(text)
        case DATE_REGEX(_*) => DATE_FORMAT.parse(text)
        case TIMEASKEY_REGEX(_*) => TIMEASKEY_FORMAT.parse(text)
        case DATEASKEY_REGEX(_*) => DATEASKEY_FORMAT.parse(text)
        case YYYYMM_REGEX(_*) => YYYYMM_FORMAT.parse(text)
        case HHMMSS_REGEX(_*) => HHMMSS_FORMAT.parse(text) //新增,处理hh:mi:ss格式
        case HHMM_REGEX(_*) => HHMMSS_FORMAT2.parse(text) //新增,处理hh:mi格式
        case _ => null
      }
    } else {
      null
    }
  }

  /**
    * 将字符串转换为时间戳
    *
    * @param text 源字符串
    * @return
    */
  def parseTimeStamp(text: String): Timestamp = {
    val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val TIME_FORMAT2 = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss")
    val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
    val TIMEASKEY_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss")
    val DATEASKEY_FORMAT = new SimpleDateFormat("yyyyMMdd")
    text match {
      case TIME_REGEX(_*) => new Timestamp(TIME_FORMAT.parse(text).getTime)
      case TIME_REGEX2(_*) => new Timestamp(TIME_FORMAT2.parse(text).getTime)
      case DATE_REGEX(_*) => new Timestamp(DATE_FORMAT.parse(text).getTime)
      case TIMEASKEY_REGEX(_*) => new Timestamp(TIMEASKEY_FORMAT.parse(text).getTime)
      case DATEASKEY_REGEX(_*) => new Timestamp(DATEASKEY_FORMAT.parse(text).getTime)
      case _ => null
    }
  }

  /**
    * 将时间转换为字符串
    *
    * @param date    时间
    * @param pattern 传入时间的模式:
    *                TIME-时间格式(YYYY-MM-DD HH:MM:SS)
    *                DATE-日期格式(YYYY-MM-DD)
    *                TIMEASKEY-时间作为KEY的格式(YYYYMMDDHHMMSS)
    *                DATEASKEY-日期作为KEY的格式(YYYYMMDD)
    * @return
    */
  def  format(date: Date, pattern: String): String = {
    pattern match {
      case "TIME" => TIME_FORMAT.format(date)
      case "DATE" => DATE_FORMAT.format(date)
      case "DATETIME" => DATETIME_FORMAT.format(date)
      case "HOURSASKEY" => HOURSASKEY_FORMAT.format(date)
      case "TIMEASKEY" => TIMEASKEY_FORMAT.format(date)
      case "DATEASKEY" => DATEASKEY_FORMAT.format(date)
      case "MONTHASKEY" => MONTHASKEY_FORMAT.format(date)
      case "HHMMSS" => HHMMSS_FORMAT.format(date)
      case _ => null
    }
  }

  /**
    * 依据出入的模式将时间转换为字符串
    *
    * @param date    时间
    * @param pattern 传入时间的模式:
    * @return
    */
  def formatByPattern(date: Date, pattern: String): String = {

    //最终结果
    var retTime = ""

    //格式化数据
    try {
      val format = FastDateFormat.getInstance(pattern)
      retTime = format.format(date);
    } catch {
      case e: Throwable => log.error(s"Exception e : $e")
    }

    //统一处理 "" 处理为null
    if ("" == retTime) {
      retTime = null
    }
    retTime
  }

  /**
    * 将时间转换为字符串
    *
    * @param date1   时间字符串
    * @param pattern 传入时间的模式:
    *                TIME-时间格式(YYYY-MM-DD HH:MM:SS)
    *                DATE-日期格式(YYYY-MM-DD)
    *                TIMEASKEY-时间作为KEY的格式(YYYYMMDDHHMMSS)
    *                DATEASKEY-日期作为KEY的格式(YYYYMMDD)
    * @return
    */
  def format(date1: String, pattern: String): String = {
    if (date1 != null && !date1.equals("null")) {
      val date = parse(date1)
      if (date != null) {
        pattern match {
          case "TIME" => {
            date1.r match {
              case TIME_REGEX(_*) => TIME_FORMAT.format(date)
              case TIME_REGEX2(_*) => TIME_FORMAT2.format(date)
              case _ => TIME_FORMAT.format(date)
            }
          }
          case "DATE" => DATE_FORMAT.format(date)
          case "MONTH" => MONTH_FORMAT.format(date)
          case "DATETIME" => DATETIME_FORMAT.format(date)
          case "TIMEASKEY" => TIMEASKEY_FORMAT.format(date)
          case "MINSASKEY" => MINSASKEY_FORMAT.format(date)
          case "HOURSASKEY" => HOURSASKEY_FORMAT.format(date)
          case "MONTHASKEY" => MONTHASKEY_FORMAT.format(date)
          case "DATEASKEY" => DATEASKEY_FORMAT.format(date)
          case "HHMMSS" => HHMMSS_FORMAT.format(date)
          //        新增
          case "YYMMDDHHMM" => YYMMDDHHMM_FORMAT.format(date)
          case "YYMMDD_HH" => YYMMDDHH_FORMAT.format(date)
          case "YY-MM-DD HH:00:00" => YYMMDDHH0000_FORMAT.format(date)

          //新增
          case "yyyy-MM-dd HH:mm:ss" => TIME_FORMAT.format(date)
          case _ => null
        }
      } else {
        null
      }
    } else {
      null
    }
  }

  /**
    * 计算时间差
    *
    * @param time1 时间字符串
    * @param time2 时间字符串
    * @return 时间差的分钟数
    */
  def minus(time1: String, time2: String): java.lang.Double = {
    if (time1 != null && time2 != null && !time2.equals("null")) {
      val dateTime1 = parse(time1);
      val dateTime2 = parse(time2);
      if (dateTime1 != null && dateTime2 != null)
        ((dateTime1.getTime - dateTime2.getTime).toDouble / 1000 / 60)
      else null
    } else {
      null
    }

  }

  /**
    * 计算时间差
    *
    * @param time1 时间字符串
    * @param time2 时间字符串
    * @return 时间差的毫秒数
    */
  def minus(time1: Date, time2: Date): java.lang.Double = {
    if (time1 != null && time2 != null)
      (time1.getTime - time2.getTime).toDouble
    else null

  }


  /**
    * 对时间增加天数
    *
    * @param time1
    * @param days
    * @return
    */
  def addDays(time1: Date, days: Integer, pattern: String): String = {
    if (time1 != null) {
      var cal = Calendar.getInstance()
      cal.setTime(time1)
      cal.add(Calendar.DATE, days)
      pattern match {
        case "DATE" => format(cal.getTime, "DATE")
        case _ => format(cal.getTime, "TIME")
      }
    } else {
      null
    }
  }

  /**
    * 对时间增加天数
    *
    * @param time1
    * @param days
    * @return
    */
  def addDays(time1: String, days: Integer, pattern: String): String = {
    if (time1 != null && !"".equals(time1)) {
      var time2 = parse(time1)
      if (time2 != null) {
        var cal = Calendar.getInstance()
        cal.setTime(time2)
        cal.add(Calendar.DATE, if (days == null) 0 else days)
        pattern match {
          case "DATE" => format(cal.getTime, "DATE")
          case "DATETIME" => format(cal.getTime, "DATETIME")
          case "DATEASKEY" => format(cal.getTime, "DATEASKEY")
          case _ => format(cal.getTime, "TIME")
        }
      } else {
        null
      }
    } else {
      null
    }
  }

  /**
    * 对时间增加天数且天数为浮点数
    *
    * @param time1
    * @param days
    * @return
    */
  def addDays(time1: String, days: java.lang.Double, pattern: String): String = {
    if (time1 != null && !"".equals(time1)) {
      var time2 = parse(time1)
      if (time2 != null) {
        var cal = Calendar.getInstance()
        cal.setTime(time2)
        cal.add(Calendar.MINUTE, if (days == null) 0 else (days * 24 * 60).toInt)
        pattern match {
          case "DATE" => format(cal.getTime, "DATE")
          case "DATETIME" => format(cal.getTime, "DATETIME")
          case "DATEASKEY" => format(cal.getTime, "DATEASKEY")
          case _ => format(cal.getTime, "TIME")
        }
      } else {
        null
      }
    } else {
      null
    }
  }


  /**
    * 对时间增加小时
    *
    * @param time1
    * @param hours
    * @return
    */
  def addHours(time1: Date, hours: Integer, pattern: String): String = {
    if (time1 != null) {
      var cal = Calendar.getInstance()
      cal.setTime(time1)
      cal.add(Calendar.HOUR, if (hours == null) 0 else hours)
      pattern match {
        case "DATE" => format(cal.getTime, "DATE")
        case _ => format(cal.getTime, "TIME")
      }
    } else {
      null
    }

  }

  /**
    * 对时间增加小时
    *
    * @param time1
    * @param hours
    * @return
    */
  def addHours(time1: String, hours: String, pattern: String): String = {
    val hour = hours.toDouble
    addHours(time1, hour, pattern)
  }

  /**
    * 对时间增加小时
    *
    * @param time1
    * @param hours
    * @return
    */
  def addHours(time1: String, hours: Integer, pattern: String): String = {
    if (time1 != null && !"".equals(time1)) {
      var cal = Calendar.getInstance()
      var time2 = parse(time1)
      cal.setTime(time2)
      cal.add(Calendar.HOUR, if (hours == null) 0 else hours)
      pattern match {
        case "DATE" => format(cal.getTime, "DATE")
        case _ => format(cal.getTime, "TIME")
      }
    } else {
      null
    }

  }

  /**
    * 对时间增加小时（小时的类型为浮点数）
    *
    * @param time1
    * @param hours
    * @return
    */

  def addHours(time1: String, hours: java.lang.Double, pattern: String): String = {
    if (time1 != null && !"".equals(time1)) {
      var cal = Calendar.getInstance()
      var time2 = parse(time1)
      cal.setTime(time2)
      cal.add(Calendar.MINUTE, if (hours == null) 0 else (hours * 60).toInt)
      pattern match {
        case "DATE" => format(cal.getTime, "DATE")
        case _ => format(cal.getTime, "TIME")
      }
    } else {
      null
    }

  }


  /**
    * 对时间增加分钟
    *
    * @param time1
    * @param minutes
    * @return
    */
  def addMinutes(time1: String, minutes: Integer, pattern: String): String = {
    if (time1 != null && !"".equals(time1) && !time1.equals("null") && !time1.equals("NULL")) {
      var cal = Calendar.getInstance()
      var time2 = parse(time1)

      cal.setTime(time2)
      cal.add(Calendar.MINUTE, if (minutes == null || minutes.equals("null")) 0 else minutes)
      pattern match {
        case "DATE" => format(cal.getTime, "DATE")
        case _ => format(cal.getTime, "TIME")
      }
    } else {
      null
    }
  }

  /**
    * 对时间增加秒
    *
    * @param time1
    * @param seconds
    * @return
    */
  def addSeconds(time1: String, seconds: Integer, pattern: String): String = {
    if (time1 != null && !"".equals(time1)) {
      var cal = Calendar.getInstance()
      var time2 = parse(time1)
      cal.setTime(time2)
      cal.add(Calendar.SECOND, if (seconds == null) 0 else seconds)
      pattern match {
        case "DATE" => format(cal.getTime, "DATE")
        case _ => format(cal.getTime, "TIME")
      }
    } else {
      null
    }
  }
}
