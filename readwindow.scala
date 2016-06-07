import org.apache.spark.streaming.{Seconds, StreamingContext,Minutes}
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.HashPartitioner
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.kafka._

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import java.util.HashMap
import java.util.ArrayList
import java.util.Iterator

object  readwindow   {
  

  //define table fields
  def main(args: Array[String]) {
	
        //Create a Streamingcontext through a SparkContext
	val checkpointDir = "checkpoint1"
	val sparkConf = new SparkConf().setAppName("data_topic_ems").set("spark.ui.port","4050")
	val sc = new SparkContext(sparkConf)
        val accum = sc.accumulator(0)


   	val ssc = new StreamingContext(sc,Seconds(5))

	val sqlContext = new SQLContext(sc)
        val KafkaIP  = "202.1.2.103:2181"	
	//val KafkaIP = "202.1.2.152:2181,202.1.2.141:2181,202.1.2.151:2181" //"10.107.217.142:2182" 
	var topics = List(("EMS",1)).toMap
	val stream1 = KafkaUtils.createStream(ssc, KafkaIP, "windowtest", topics).map(_._2)
	stream1.window(Seconds(15),Seconds(15)).print()
        println("****************************************")
	stream1.print()
	//val stream1 = ssc.socketTextStream("localhost",9996)

	//getProducer
	
/*	 stream1.foreachRDD(rdd=>{
	if(rdd.count >0 ){
	    println("sava bigan")
            val instancetime = System.currentTimeMillis() 
	    val ems = rdd
	    val hw_ems = rdd.filter(x => x.split(",")(9) == "263671")
	    val hw_wuxian_ems = rdd.filter(x => x.split(",")(1).length >1).filter( x => x.split(",")(9)=="263671" && x.split(",")(1).substring(0,1) == "G")
            ems.saveAsTextFile("MesBarcodePtion/ems" + instancetime.toString)
	    //ems.saveAsTextFile("zhanliming/windowtest/hha")
	    hw_ems.saveAsTextFile("MesBarcodePtion_Manufature#HW/ems_hw" + instancetime.toString)  
	    hw_wuxian_ems.saveAsTextFile("MesBarcodePtion_Manufature#HW_Productline#Wireless/ems_hw_wireless" + instancetime.toString )
	    println("end")
        }

        })*/
        ssc.start()
        ssc.awaitTermination()
  }
}
