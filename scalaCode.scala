package com.walmart.spark.job.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter

import com.datastax.driver.core.Row
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.Config
import com.walmart.common.config.{AppConfig, Status}
import com.walmart.spark.job.Job
import com.walmartlabs.smartsourcing.common.{MailClient, SparkJobInfoPayload}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_list, collect_set, lit, udf, unix_timestamp}
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json.JSONObject

import scala.util.parsing.json.JSONArray

class CarrierCapacityForecast extends Job with Serializable{

  override def run(env: String, configuration: Config, args: Array[String]): Unit = {
    val sparkJobInfoPayload = new SparkJobInfoPayload()
    sparkJobInfoPayload.setAppName(Constants.populateTransportationMapJob)
    sparkJobInfoPayload.setEnv(env)
    val date = DateTime.now()
    val jobDate = date.toString("yyyyMMddHH")
    sparkJobInfoPayload.setJobTime(jobDate)
    println("JobStart: " + date)

    val objectMapper: ObjectMapper = new ObjectMapper
    try {

      val stringify = udf { vs: Seq[Seq[String]] =>
        var str = ""
        if (vs != null && vs.nonEmpty) {
          for (seq <- vs) {
            if (seq != null && seq.nonEmpty)
              str += s"""${seq.mkString(",")}"""
          }
        }
        str
      }


      val timeStampify = udf { x: String =>
//        val f = DateTimeFormat.forPattern("yyyy-mm-dd hh:mm:ss")
//        val dateTime = f.parseDateTime(x)
        val timeStamp = Timestamp.valueOf(x)
        timeStamp
      }

      val cuttofy = udf(( esd: String, cutoff: String) => {
        var eddStart = Timestamp.valueOf(esd)
        val duration = ((cutoff.substring(0, 2).toLong * 60 * 60) + (cutoff.substring(2,4).toLong)) * 1000
        eddStart.setTime(eddStart.getTime() + duration)
        eddStart
      })

      val findDiffa = udf((x: Timestamp, y: Timestamp) => {
        ((x.getTime - y.getTime) / 1000 ) / 60
      })

      val env = args(0)
      val configuration = AppConfig.loadConfig(env)

      val conf = new SparkConf()
        .setAppName("Carrier Capacity Forecast")
        .set("spark.cassandra.connection.host", configuration.getString("cassandra.cdc.host.urls"))
        .set("spark.cassandra.connection.port", configuration.getString("cassandra.port"))
        .set("spark.cassandra.connection.connections_per_executor_max", configuration.getString("cassandra.max.conns.perhost"))
        .set("spark.cassandra.connection.reconnection_delay_ms.max", configuration.getString("cassandra.reconnect.max.delay"))
        .set("spark.cassandra.connection.reconnection_delay_ms.min", configuration.getString("cassandra.reconnect.base.delay"))
        .set("spark.cassandra.output.consistency.level", configuration.getString("cassandra.default.write.consistency"))
        .set("spark.cassandra.input.consistency.level", configuration.getString("cassandra.default.read.consistency"))
        .set("spark.cassandra.input.fetch.size_in_rows", configuration.getString("cassandra.fetch.size"))
        .set("spark.executor.extraJavaOptions", "-Djava.security.egd=file:///dev/urandom")
      //.set("spark.cassandra.auth.username", configuration.getString("cassandra.username"))
      //.set("spark.cassandra.auth.password", configuration.getString("cassandra.password"))

      if (env == "prod") {
        conf.setMaster(configuration.getString("spark.master"))
      }
      // Initializing the spark session
      val spark = SparkSession.builder().config(conf).getOrCreate()

      // Fetching Data from Oracle Tables
      val joinQuery = "(SELECT to_timestamp(to_char(from_tz(CAST(el.created_on AS TIMESTAMP), 'UTC') AT TIME ZONE 'PST','dd-mon-yyyy hh24:mi'),'dd-mon-yyyy hh24:mi') as created_on, \n--e2.created_on as x,\n      --cg.carrier_group_name,\n      --el.ship_node,\n      el.EXPECTED_SHIP_DATE,\n      --el.shipment_method,\n      --el.carrier_method_id,\n      --cga.assigned_capacity,\n      SUM(consumed_capacity) total, \n      SUM(SUM(consumed_capacity)) over ( PARTITION BY cg.carrier_group_name ORDER BY cg.carrier_group_name,to_timestamp(to_char(from_tz(CAST(el.created_on AS TIMESTAMP), 'UTC') AT TIME ZONE 'PST','dd-mon-yyyy hh24:mi'),'dd-mon-yyyy hh24:mi')) AS running_total,\n      MAX(CASE WHEN to_char(el.expected_ship_date,'DY') = 'SUN' THEN nvl(sun_ship_cutoff_time,'2359')\n               WHEN to_char(el.expected_ship_date,'DY') = 'MON' THEN nvl(mon_ship_cutoff_time,'2359')\n               WHEN to_char(el.expected_ship_date,'DY') = 'TUE' THEN nvl(tue_ship_cutoff_time,'2359')\n               WHEN to_char(el.expected_ship_date,'DY') = 'WED' THEN nvl(wed_ship_cutoff_time,'2359')\n               WHEN to_char(el.expected_ship_date,'DY') = 'THU' THEN nvl(thu_ship_cutoff_time,'2359')\n               WHEN to_char(el.expected_ship_date,'DY') = 'FRI' THEN nvl(fri_ship_cutoff_time,'2359')\n               WHEN to_char(el.expected_ship_date,'DY') = 'SAT' THEN nvl(sat_ship_cutoff_time,'2359')\n          END) cutoff_time\nFROM\ncarrier_grp_assigned_cap cga,\nCARRIER_GROUP cg,\nCARRIER_GROUP_CAP_ORD_EVNT_LOG  el,\n--CARRIER_GROUP_CAP_ORD_EVNT_LOG e2,\nFOCI_DISTRIBUTOR_CARRIER DC\nWHERE\ncga.ship_node = cg.ship_node\nAND CG.SHIP_NODE = DC.ship_node(+)\nAND cg.carrier_method_id = dc.carrier_service_code(+)\nAND cga.carrier_group_name = cg.carrier_group_name\nAND el.ship_node = cg.ship_node\nAND el.EXPECTED_SHIP_DATE = cga.EXPECTED_SHIP_DATE\nAND el.shipment_method = cg.ship_method\nAND el.carrier_method_id = cg.carrier_method_id  \n--AND el.CREATED_ON = e2.CREATED_ON\n--AND cg.group_type = 'Trailer_cap'                                                                  \nAND CG.CARRIER_GROUP_NAME not like 'sys_%'\nAND TRUNC(el.EXPECTED_SHIP_DATE) = to_date('01-07-19','DD-MM-YY')\nAND cga.carrier_group_name like '%PHL1%'\nGROUP BY\ncg.carrier_group_name,\nel.ship_node,\nel.EXPECTED_SHIP_DATE,\nel.shipment_method,\nel.carrier_method_id,\ncga.assigned_capacity,\nto_timestamp(to_char(from_tz(CAST(el.created_on AS TIMESTAMP), 'UTC') AT TIME ZONE 'PST','dd-mon-yyyy hh24:mi'),'dd-mon-yyyy hh24:mi')\nORDER BY\ncg.carrier_group_name,to_timestamp(to_char(from_tz(CAST(el.created_on AS TIMESTAMP), 'UTC') AT TIME ZONE 'PST','dd-mon-yyyy hh24:mi'),'dd-mon-yyyy hh24:mi'))xxx"
      val fociData = spark.read.format("jdbc")
        .option("url", configuration.getString("ccap.url"))
        .option("dbtable", joinQuery)
        .option("driver", configuration.getString("oracleDriver"))
        .option("user", configuration.getString("ccap.username"))
        .option("password", configuration.getString("ccap.password")).load().limit(100)

      fociData.persist().show()
//      val test = fociData.withColumn("new_column", unix_timestamp(fociData("CREATED_ON")).cast("timestamp"))
//      fociData.withColumn("nc", timeStampify(fociData("CREATED_ON"))).show()
      val goodData = fociData.withColumn("cutoffDiffs", cuttofy(fociData("EXPECTED_SHIP_DATE"), fociData("CUTOFF_TIME")))
      goodData.withColumn("diffa", findDiffa(goodData("cutoffDiffs"), goodData("CREATED_ON"))).show()
      fociData.unpersist()
      
    } catch {
      case exception: Exception =>
        sparkJobInfoPayload.setStatus(Status.FAILED.toString)
        sparkJobInfoPayload.setError(exception.getMessage)
        println(exception.getMessage)
        exception.printStackTrace()
    } finally {
      val enddate = DateTime.now()
      println("JobEndDate: " + enddate)
      try {
//        MailClient.sendMail(Constants.populateTransportationMapJob + sparkJobInfoPayload.getStatus, objectMapper.writeValueAsString(sparkJobInfoPayload), env)
      } catch {
        case exception: Exception =>
          println(exception.getMessage)
      }
    }
  }

}
