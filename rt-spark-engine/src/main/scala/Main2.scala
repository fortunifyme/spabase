
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
//import org.apache.spark.streaming.kafka.KafkaUtils
// import org.apache.spark.streaming.kafka._
import it.nerdammer.spark.hbase._
import model.{Reading, Trajectory}
import redis.CacheService
//import integration.KafkaMessageProducer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


object Main2 {
	case class Record(rowKey: String, id: String, ts: String)

	def main(args: Array[String]) : Unit  = {


	}


}


