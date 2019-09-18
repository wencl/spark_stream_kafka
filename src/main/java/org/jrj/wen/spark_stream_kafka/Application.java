package org.jrj.wen.spark_stream_kafka;

import org.apache.log4j.Logger;
import org.jrj.wen.spark_stream_kafka.execute.JavaDirectKafkaWordCount;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 
 * @author chenglong.wen
 *
 */
@SpringBootApplication
@EnableScheduling
public class Application implements CommandLineRunner {
	private static Logger logger = Logger.getLogger(Application.class);

	public static void main(String[] args) {

		// SpringApplication.run(Application.class, args);
		SpringApplication app = new SpringApplication(Application.class);
		app.setBannerMode(Banner.Mode.OFF);
		app.run(args);
		logger.info("Application start !");
		
	}

	@Override
	public void run(String... args) throws Exception {
		// TODO Auto-generated method stub
		logger.info("Application JavaDirectKafkaWordCount start !");
		JavaDirectKafkaWordCount.aa();
	}

}
