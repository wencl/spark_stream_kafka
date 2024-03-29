package org.jrj.wen.spark_stream_kafka.execute;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount. Usage:
 * JavaDirectKafkaWordCount <brokers> <groupId> <topics> <brokers> is a list of
 * one or more Kafka brokers <groupId> is a consumer group name to consume from
 * topics <topics> is a list of one or more kafka topics to consume from
 *
 * Example: $ bin/run-example streaming.JavaDirectKafkaWordCount
 * broker1-host:port,broker2-host:port \ consumer-group topic1,topic2
 */

public class JavaDirectKafkaWordCount {
	private static Logger logger = Logger.getLogger(JavaDirectKafkaWordCount.class);
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void aa() {

		logger.info("JavaDirectKafkaWordCount start...");
		String brokers = "hadoop-1:9092,hadoop-2:9092,hadoop-3:9092,hadoop-4:9092";
		String groupId = "spark_stream_kafka";
		String topics = "spark_stream_kafka";

		// Create context with a 2 seconds batch interval
		SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
		SparkContext sc = new SparkContext(sparkConf);

		// try {
		// FileSystem fs = FileSystem.newInstance(sc.hadoopConfiguration());
		// Path p = new Path("/spark/spark_stream_kafka-0.0.1-SNAPSHOT.jar");
		// fs.deleteOnExit(p);
		// fs.copyFromLocalFile(false,
		// new
		// Path("/usr/local/wen/spark_stream_kafka/target/original-spark_stream_kafka-0.0.1-SNAPSHOT.jar"),
		// p);
		// logger.info("上传jar成功");
		// } catch (IOException e1) {
		// // TODO Auto-generated catch block
		// e1.printStackTrace();
		// }

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		// Create direct kafka stream with brokers and topics
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

		// Get the lines, split them into words, count the words and print
		JavaDStream<String> lines = messages.map(ConsumerRecord::value);
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
				.reduceByKey((i1, i2) -> i1 + i2);
		// wordCounts.print();

		// （窗口长度）window length – 窗口覆盖的时间长度（上图中为3）
		// （滑动距离）sliding interval – 窗口启动的时间间隔（上图中为2）
		// 注意，这两个参数都必须是 DStream 批次间隔（上图中为1）的整数倍.
		JavaPairDStream<String, Integer> windowedWordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
				.reduceByKeyAndWindow((i1, i2) -> i1 + i2, Durations.seconds(10), Durations.seconds(6));
		// windowedWordCounts.print();

		SparkSession ss = SparkSession.builder().appName("wordCountSparkSql").config(sparkConf).getOrCreate();
		Encoder<String> stringEncoder = Encoders.STRING();
		Dataset<String> primitiveDS = ss.createDataset(Arrays.asList("d32321d:100", "1d32:200", "d32d:300"),
				stringEncoder);
		JavaPairRDD<String, Integer> jr = primitiveDS.javaRDD()
				.mapToPair((PairFunction<String, String, Integer>) str -> {
					String[] arr = str.split(":");
					return new Tuple2<>(arr[0], Integer.valueOf(arr[1]));
				});
		// JavaPairDStream<String, Integer> windowedStream =
		// wordCounts.window(Durations.seconds(20));
		JavaPairDStream<String, Integer> joinedStream = windowedWordCounts.transformToPair(rdd -> rdd.join(jr))
				.mapToPair(
						(PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, Integer>) p -> new Tuple2<String, Integer>(
								p._1(), p._2()._1() + p._2()._2()));

		// Start the computation

		joinedStream.print();

		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger.info("JavaDirectKafkaWordCount end...");
	}
}
