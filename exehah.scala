import org.apache.spark.streaming.{Seconds, StreamingContext,Minutes}
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.HashPartitioner
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.kafka._
import org.apache.spark.sql.SQLContext
import scala.util.control.Breaks._
import java.text.SimpleDateFormat
import scala.reflect.runtime.universe._
import java.util.Date
import scala.collection.mutable.ArrayBuffer
import java.util.HashMap
import java.util.Map
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object BarcodeRank{

  //define table fields
	case class DPC(itemcode:String, code:String)	
	case class BPL(boardcode:String, barcode:String, code:String, itemcode:String, num:Long)
	case class numP(barcode:String, num:Long)
	case class piciP(barcode:String, bach:String, numb:Long)
	case class rank(boardcode:String, barcode:String, numb:Long, num:Long, bach:String, numw:Long)
	case class proced(barcode:String)
  def main(args: Array[String]) {

        //Create a Streamingcontext through a SparkContext
	val checkpointDir = "checkpoint1"
	val sparkConf = new SparkConf().setAppName("BarcodeRank")
        sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        sparkConf.set("spark.ui.port","4444")
       // sparkConf.set("spark.kryoserializer.buffer.max","10g")
       // sparkConf.set("spark.kryoserializer.buffer.max.mb","1068")
       // sparkConf.set("spark.kryoserializer.buffer.mb","256")
        sparkConf.set("spark.sql.shuffle.partitions","200")
//	sparkConf.set("spark.shuffle.service.enabled","true")
 //       sparkConf.set("spark.dynamicAllocation.enabled","true")
//	sparkConf.set("spark.dynamicAllocation.minExecutors","50")
	//sparkConf.set("spark.dynamicAllocation.initialExecutors","200")
//	sparkConf.set("spark.dynamicAllocation.maxExecutors","200")
	val sc = new SparkContext(sparkConf)
        val accum = sc.accumulator(0)

   	val ssc = new StreamingContext(sc,Minutes(30))

	val sqlContext = new SQLContext(sc)
        	import sqlContext.implicits._

    //    val KafkaIP  = "202.1.2.103:2181"
	val KafkaIP = "202.1.2.152:2181,202.1.2.141:2181,202.1.2.151:2181" //"10.107.217.142:2182"
	var topics = List(("BARPOOL",1)).toMap
	val stream1 = KafkaUtils.createStream(ssc, KafkaIP, "BarcodeRank", topics).map(_._2)
	stream1.print()
	//val bar_par = sc.textFile("zhanliming/barpool.txt").map(_.split(",")).map(x => BPL(x(0),x(1),x(2),x(3),x(4).toLong)).toDF()
 	//bar_par.saveAsParquetFile("zhanliming/Rankbase.parquet")
	//var mkupor = 1
	//val qingkong = sc.textFile("zhanliming/barpool.txt").map(_.split(",")).map(x => rank(x(0),x(1),x(2).toLong,x(3).toLong,x(4),mkupor.toLong)).toDF()

	val rank_res = sqlContext.parquetFile("zhanliming/Ranking2.parquet")//.repartition(200)
	rank_res.show
	rank_res.registerTempTable("rank")
	//qingkong.insertInto("rank",true)

/**read dangerous pici**/
	stream1.foreachRDD(rdd =>{
		if(rdd.count>0){
        val readpic = new SeriesCacheBase("202.1.2.108",7000)

   	val dangerM = readpic.getAllCompareMess("MES")
	//val dangerW = readpic.getAllCompareMess("WMS")
        //val danger = dangerM ++ dangerW
        val dangerdd = sc.parallelize(dangerM, 1).distinct.map(_.split("`")).map(x => DPC(x(0),x(1))).toDF()//.repartition(1)
        dangerdd.show
	dangerdd.registerTempTable("dpc")
/**read processed barcode**/
	val getCode = new SaveSampledBar("202.1.2.108",7000)
	val processedCode = getCode.getSampleSuccessBar()
	val failedCode = getCode.getSampleFailBar()
	val allDropCode = processedCode ++ failedCode
	val paralallDropCode = sc.parallelize(allDropCode, 1).distinct.map(x => proced(x)).toDF() //.repartition(1)//.distinct
	paralallDropCode.show
	paralallDropCode.registerTempTable("DCD")

/**read barcode**/
  	val barpool = sqlContext.parquetFile("zhanliming/Rankbase3.parquet")//.repartition(400)

//	val readbar = new BarcodeCacheBase("202.1.2.108",7000)
//	val barpool_raw = readbar.readSavedBoardMess_WithBom(null,0)
//	val barpool = sc.parallelize(barpool_raw,200).distinct().map(_.split("`")).map(x => BPL(x(0),x(1),x(2),x(4),x(6).toLong)).toDF()
	println("this is history")	
	barpool.show	
	//barpool.registerTempTable("bpl")
	//zhi qu lot
	//val new_bar1 = rdd.map(_.split("`")).filter(x => x(8)!="null").map(x => BPL(x(0),x(1),x(6),x(4),x(8).toLong)).toDF().distinct
	val new_bar = rdd.map(_.split("`")).filter(x => x(8)!="null").map(x => BPL(x(0),x(1),x(7),x(4),x(8).toLong)).toDF().distinct //(numPartitions =200)
//	val new_bar  = new_bar2.distinct //.unionAll(new_bar2).distinct
	println("this is new data")
	new_bar.show
	if(new_bar.count>0){
	//new_bar.insertInto("bpl",false)
	barpool.registerTempTable("bpl")
	new_bar.repartition(1).insertInto("bpl",false)
//	val dropprocedbar = sqlContext.sql("select bl.boardcode, bl.barcode, bl.code, bl.itemcode, bl.num from  bpl bl,DCD dd where  dd.barcode <> bl.barcode")
//	dropprocedbar.show
//	dropprocedbar.registerTempTable("bpld")	
        val target = sqlContext.sql("select bl.boardcode, bl.barcode, bl.code, bl.itemcode, bl.num from dpc dc, bpl bl where dc.itemcode = bl.itemcode and dc.code = bl.code ").distinct//.map(x => x.mkString("`"))
	println("this is filter danger pici:")
	target.show
	target.registerTempTable("base")
	val boardbase = sqlContext.sql("select bs.boardcode, bs.barcode from base bs,DCD dd where bs.barcode <> dd.barcode").distinct
	println("this is unprocessed barcode:")
	boardbase.show
	boardbase.registerTempTable("board")
	val calcubase = target.map(x => x.mkString("`")).map(_.split("`")).persist
	val numpair = calcubase.map(x => (x(1),x(4).toLong)).reduceByKey(_+_).map(x => numP(x._1,x._2.toInt)).toDF().distinct
        numpair.registerTempTable("NUM")
	val picipair = calcubase.map(x =>(x(1),x(3)+"$"+x(2)+"$"+x(4))).groupByKey().map{x => piciP(x._1,x._2.mkString(","),x._2.toList.size.toLong)}.toDF().distinct//map{case(x,y) => x+"`"+y.mkString(",")+"`"+y.toList.size}
	  picipair.registerTempTable("PC")
        val result_raw = sqlContext.sql("select bs.boardcode,pc.barcode,pc.numb,num.num,pc.bach,(pc.numb*10000+num.num) as numw from board bs, NUM num, PC pc where bs.barcode = num.barcode and bs.barcode = pc.barcode and num.barcode = pc.barcode").distinct.sort($"boardcode",$"numw".desc)//.repartition(1)
  	println("this is result:")
	result_raw.show
	result_raw.registerTempTable("randn")
        result_raw.insertInto("rank",true)
	     val top10 = sc.textFile("zhanliming/top10.txt").collect.toList//序列号，09条码
            for(bom <- top10){
		val temp = sqlContext.sql("select rn.boardcode,rn.barcode,rn.numb,rn.num,rn.bach,rn.numw from randn rn where rn.boardcode="+"'"+bom.toString+"'")
		temp.show
		if(temp.count>0){
		val putway = new saveSeriesDensity("202.1.2.108",7000)
		putway.delOrderBoardMess(bom.toString)
 		println(s"youzhegebom:$bom")
		temp.map(x =>x.mkString("`")).foreachPartition(records =>{

		val putway1 = new saveSeriesDensity("202.1.2.108",7000)
               records.foreach(x => {



		putway1.writeOrderBoardMess_SingleRow(x)



		})

		})
		}else{println(s"meizhege:$bom")}
	}
//	AccurateSamplingObj.AccurateSamplingFunc()

}else{println("nothing1")}
}else {println("nothing")}
  	//val result_raw = sqlContext.parquetFile("zhanliming/Ranking.parquet")
	//result_raw.show
	//val inredis = result_raw.map(x => x(0)+"`"+x(1)+"`"+x(2)+"`"+x(3)+"`"+x(4))//.collect.toList
        //println(Integer.MAX_VALUE)
//	inredis.foreachPartition(rdd =>{
//	val rdd1 = rdd.toList	
      	//val company_list: java.util.List[String] = ListBuffer(inredis: _*)
     //   putway.writeOrderBoardMess(company_list) 
//	})
//Thread.sleep(50000)
  
	})

        ssc.start()
        ssc.awaitTermination()
 

   }
}
