/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.storage._

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

case class Recipient(
    messageId: String,
    recipientRealName: String,
    recipientEmailAddress: String,
    recipientType: String,  // Hmmm, perhaps replace with case class
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
  
  val canonStr = (s: String) => s.toLowerCase

  val toCanonStr = udf(canonStr)

  val coalesceStr = (s1: String, s2: String) => if (emptyStr(s1)) s2 else s1 

  val coalesceStrUDF = udf(coalesceStr)

  // Good example of abstraction / reuse that is super painful in SQL:
  def addrNameCounts(ms: DataFrame,addrColName: String, realColName: String): DataFrame = {
    val addrNameCounts = ms
            .select(toCanonStr(ms(addrColName)).as("emailAddress"),
                   coalesceStrUDF(ms(realColName),ms("fromEmailAddress")).as("correspondentName")) 
            .groupBy("emailAddress","correspondentName")
            .count()
            .sort("emailAddress","count")

    return addrNameCounts    
  }


  def fromNameCounts(ms: DataFrame): DataFrame = {
    addrNameCounts(ms,"fromEmailAddress","fromRealName")
  }

  def recipNameCounts(ms: DataFrame, rs: DataFrame): DataFrame = {
    val jms = ms.join(rs,ms("messageId") === rs("messageId") ).persist(StorageLevel.OFF_HEAP)

    addrNameCounts(jms,"recipientEmailAddress", "recipientRealName")
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("CSV Test")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //val psvFile = "/Users/antony/home/src/spark/CSVTest/messages.psv"
    val psvFile = "/Users/antony/Downloads/redshift-snap/messages_snap_021115_*"

    val records = sc.newAPIHadoopFile(
      psvFile,
      classOf[RedshiftInputFormat],
      classOf[java.lang.Long],
      classOf[Array[String]])

    val messagesRDD = records.map(p => p._2)
                       .map(m => Message(m(0),m(1),parseTimestamp(m(2)),m(3),m(4),m(5),m(6),
                                      // sizeEstimate: kill it! m(7).toInt,
                                      parseTimestamp(m(8)),m(9),
                                      user_id = m(10).toInt,
                                      createdAt = parseTimestamp(m(11)) ) )
    val numRecords = records.count()
    println("Read " + numRecords + " messages.")

    val messages = sqlContext.createDataFrame(messagesRDD).persist(StorageLevel.OFF_HEAP)

    val recipsFile = "/Users/antony/Downloads/redshift-snap/recipients_snap_021115_*"

    val recipRecords = sc.newAPIHadoopFile(
      recipsFile,
      classOf[RedshiftInputFormat],
      classOf[java.lang.Long],
      classOf[Array[String]])

    println("Read " + recipRecords.count() + " recipient records")

    var recipsRDD = recipRecords.map(p => p._2)
            .map(r => Recipient(messageId = r(0), recipientRealName = r(1), recipientEmailAddress = r(2),
                                recipientType = r(3),
                                user_id = r(5).toInt,
                                createdAt = parseTimestamp(r(6))))

    var recipients = sqlContext.createDataFrame(recipsRDD).persist(StorageLevel.OFF_HEAP)

    val fromNamePairCounts = fromNameCounts(messages)

    val recipNamePairCounts = recipNameCounts(messages,recipients)

    fromNamePairCounts.show()

    println("To names: ")
    recipNamePairCounts.show()

    fromNamePairCounts.sample(false,0.01).take(100).foreach(println)

    // val fanp = fromNameCounts(messagesDF)
/*
    val addrCounts = messages
                      .map(m => (m.fromEmailAddress,1))
                      .reduceByKey(_ + _)

    val sortedCounts = addrCounts
                        .map(item => item.swap)
                        .sortByKey(false,1)
                        .map(item => item.swap)
*/
/*
    val recVals = fanp.take(25)

    for (r <- recVals) {
      println(r.toString)
    }
*/
  }
}