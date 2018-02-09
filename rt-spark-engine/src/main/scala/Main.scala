
import java.io.Serializable

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.api.java._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming._
//import org.apache.spark.streaming.kafka.KafkaUtils
// import org.apache.spark.streaming.kafka._
import it.nerdammer.spark.hbase._
import redis.CacheService
import model.{Reading, Trajectory}
//import integration.KafkaMessageProducer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


object Main {

	def main(args: Array[String]) : Unit  = {
	/*	// if (args.length < 1){
	//	 	println("General arguments: <consume_topic> <produce_topic> <broker_list> <zk_url>")
	//	 	System.exit(-1)
	//	 }

		val sparkConf = new SparkConf().setAppName("Sensor Data Stream Processor")
		sparkConf.set("spark.hbase.host", args(3))
		sparkConf.set("zookeeper.znode.parent", "/hbase")

		val streamingCtx = new StreamingContext(sparkConf, Milliseconds(250))

		val topics = Set("raw-gpstrajectory-data-topic") // Set(args(0))
		// val brokers = "192.168.56.106:9092" //args(2)
    val brokers = "192.168.56.106:2181" //args(2)
		val downstreamTopic = Set("result-aggregation-topic") // Set(args(1))

		val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
		val data = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingCtx, kafkaParams, topics)

		// Get the lines, split them into words
		data.map(_._2).foreachRDD(processRDD(_))

		// Start the computation
		println("*************************** next line streamingCtx.start()  ***************************")
	    streamingCtx.start()
	    streamingCtx.awaitTermination()*/


    // ========================================================== ANOTHER VERSION
    println("*************************** Inside main try 1_117 with hbase ***************************")



    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test",
      "auto.offset.reset" -> "latest"
      //"enable.auto.commit" -> (false)
    )

    val conf = new SparkConf().setAppName("DStream counter").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))

    val topicName = "raw-gpstrajectory-data-topic"
    val topics = Array(topicName)

    val stream = KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent,
      // Subscribe[String, String](topics, kafkaParams))
      Subscribe[String, String](topics, kafkaParams))

    stream.foreachRDD {
      rdd => println(" ============= Amount of elems ============= " + rdd.count)
    }
    // Get the lines, split them into words
    val lines = stream.map(_.value())
    lines.foreachRDD(processRDD(_))


    ssc.start()
    ssc.awaitTermination()
	}

	/**
	 * 
	 * process the rdd passed as parameter.
	 * processing involves
	 *	- convert to a data object
	 *	- send the data to kafka 
	 *	- save the data to hbase cf main
	 *
	 * @param rdd
	 */
	def processRDD(rdd: RDD[String]) : Unit ={
		//logic begins
		val readingRDD : RDD[Reading] = rdd.map(createReading(_))
		//cache rdd for performance
		readingRDD.cache

		val trajectoryMoveMentRDD : RDD[Trajectory] = readingRDD.mapPartitions{partitionOfRecords => {
			    val cache = new CacheService
			    val rdds = partitionOfRecords.map((reading : Reading) => {
			    	val plat = cache.getAndSet("lt"+reading.trajectoryId, reading.lat.toString)
			    	val plon = cache.getAndSet("ln"+reading.trajectoryId, reading.lon.toString)
			    	val pts = cache.getAndSet("ts"+reading.trajectoryId, reading.ts.toString)


					Trajectory(if (plat == null) reading.lat else plat.toFloat, 
						if (plon == null) reading.lon else plon.toFloat, 
						reading.lat, reading.lon, 
						if (pts == null) reading.ts else pts.toLong, 
						reading.ts, reading.trajectoryId)
			    })
			    cache.disconnect
			    rdds
			}
		}

		//save to hbase
		saveReadingToHBase(readingRDD)
		//save trajectory data to hbase
		saveTrajectoryToHBase(trajectoryMoveMentRDD)
		//inform clients
		//new KafkaMessageProducer("192.168.56.106:9092","result-aggregation-topic").sendMessage("msg=load")
	}


	/**
	 * 
	 * send the items of this rdd to a kafka topic
	 * @param rdd
	 */
	def sendToKafka(rdd: RDD[Reading]) : Unit = {
		
	}

	/**
	 * 
	 * save the items of this rdd to a hbase table
	 * @param rdd
	 */
	def saveReadingToHBase(rdd: RDD[Reading]) : Unit = {
		rdd.map(r => r.toTuple)
		 	.toHBaseTable("mvment")
		    .toColumns("trajId", "lat", "lon", "alt", "ts", "userId")
		    .inColumnFamily("main")
		    .save()
	}

	def readDataFromHBase(rdd: RDD[Reading]) : Unit = {
		rdd.map(r => r.toTuple)
			.toHBaseTable("mvment")
			.toColumns("trajId", "lat", "lon", "alt", "ts", "userId")
			.inColumnFamily("main")
			.save()
	}

	/**
	 * 
	 * save the items of this rdd to a hbase table
	 * @param rdd
	 */
	def saveTrajectoryToHBase(rdd: RDD[Trajectory]) : Unit = {
		rdd.map(r => r.toTuple)
		 	.toHBaseTable("mvment_by_traj")
		    .toColumns("plat", "plon", "lon", "lat", "dist", "pts", "ts", "tdiffs", "trajid")
		    .inColumnFamily("main")
		    .save()
	}

	def createReading(line: String) : Reading = {
		//val example  = "000,20081023025304,39.984702,116.318417,0,492,39744.1201851852,2008-10-23,02:53:04"
		val parts = line.split(",")
		Reading(parts(0), parts(1), parts(2).toFloat, parts(3).toFloat, parts(5).toFloat, parts(7), parts(8) )
	}


}


