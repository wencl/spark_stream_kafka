package org.jrj.wen.spark_stream_kafka.compoment;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.jrj.wen.spark_stream_kafka.Application;
import org.jrj.wen.spark_stream_kafka.execute.JavaDirectKafkaWordCount;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducerScheduler {
	private static Logger logger = Logger.getLogger(KafkaProducerScheduler.class);
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Scheduled(fixedRate = 24 * 60 * 60 * 60)
	public void d1() {

		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				kafkaTemplate.send("spark_stream_kafka", "1d32 d32d ds12d d32321d d32dd d32d d32d23 d32d");
			}
		}, 0, 100, TimeUnit.MILLISECONDS);
		
	}
}
