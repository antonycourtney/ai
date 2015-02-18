/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

import com.databricks.examples.redshift.input.RedshiftInputFormat

import java.sql.Timestamp

import java.text._


// messages table as a simple case class:

case class Message(
    messageId: String,
    threadId: String,
    received: Timestamp,
    fromRealName: String,
    fromEmailAddress: String,
    subject: String,
    snippet: String,
    // sizeEstimate: Int,
    date: Timestamp,
    uid: String,
    user_id: Int,
    createdAt: Timestamp
  )


object CSVTest {
  val tldf = new ThreadLocal[SimpleDateFormat]

  def getTLSDateFormat(): SimpleDateFormat = {
    var df = tldf.get
    if (df == null) {
      df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      tldf.set(df)
    }
    return df
  }
 
  def parseTimestamp(s: String): Timestamp = {
    val df = getTLSDateFormat
    try {
      if (s.length==0)
        return null
      val ts = new Timestamp(df.parse(s).getTime)
      ts
    } catch {
      case ex: Throwable => println("*** could not parse timestamp: " + s)
      return null
    }
  }

  def splitLine(ls: String): Array[String] = {
    println("splitLine: ls: ", ls)
    val parts = ls.split("\\|")
    println("parts: ", parts.mkString(", "))
    parts
  }

  def emptyStr(s: String): Boolean = {
    s==null || s.length==0
  }

  /*
   * Given an email address and its corresponding name field,
   * generate a canonical (lower case) form of the email
   * address, paired with either the realNameField contents
   * or the emailAddress if null / empty
   */
  def canonNamePair(emailAddr: String, realNameField: String): (String,String) = {
    var canonEmail = emailAddr.toLowerCase
    var corrName = if (emptyStr(realNameField)) emailAddr else realNameField
    return (canonEmail,corrName)
  }

  def fromNameCounts(messages: RDD[Message]): RDD[((String,String),Int)] = {
    val namePairs = messages
      .map(m => (canonNamePair(m.fromEmailAddress,m.fromRealName),1))
      .reduceByKey(_ + _)

    namePairs
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("CSV Test")
    val sc = new SparkContext(conf)

    //val psvFile = "/Users/antony/home/src/spark/CSVTest/messages.psv"
    val psvFile = "/Users/antony/Downloads/redshift-snap/messages_snap_021115_*"

    val records = sc.newAPIHadoopFile(
      psvFile,
      classOf[RedshiftInputFormat],
      classOf[java.lang.Long],
      classOf[Array[String]])

    val messages = records.map(p => p._2)
                    .map(m => Message(m(0),m(1),parseTimestamp(m(2)),m(3),m(4),m(5),m(6),
                                      // sizeEstimate: kill it! m(7).toInt,
                                      parseTimestamp(m(8)),m(9),
                                      m(10).toInt,
                                      parseTimestamp(m(11)) ) )
    val numRecords = records.count()
    println("Read " + numRecords + " messages.")

    val fanp = fromNameCounts(messages)
/*
    val addrCounts = messages
                      .map(m => (m.fromEmailAddress,1))
                      .reduceByKey(_ + _)

    val sortedCounts = addrCounts
                        .map(item => item.swap)
                        .sortByKey(false,1)
                        .map(item => item.swap)
*/
    val recVals = fanp.take(25)

    for (r <- recVals) {
      println(r.toString)
    }

  }
}