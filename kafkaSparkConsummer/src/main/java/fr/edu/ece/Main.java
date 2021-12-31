package fr.edu.ece;

import org.apache.kafka.common.serialization.StringDeserializer;
import fr.edu.ece.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    public static void main (String [] args) {
        Properties props = new Properties();
        try (InputStream input = new FileInputStream(new File(System.getProperty("twitter.kafka_producer.configuration")))) {

            // load a properties file
            LOGGER.info("Parsing configuration file...");
            props.load(input);

            Map<String, Object> kafkaParams = new HashMap<>();
            kafkaParams.put("bootstrap.servers", props.get("kafka.bootstrap-server"));
            kafkaParams.put("key.deserializer", StringDeserializer.class);
            kafkaParams.put("value.deserializer", StringDeserializer.class);
            kafkaParams.put("group.id", props.get("kafka.consumer.groupId"));
            kafkaParams.put("auto.offset.reset", "latest");
            kafkaParams.put("enable.auto.commit", false);

            LOGGER.info("Creating kafka consumer...");
            KafkaConsumer kafkaConsumer = new KafkaConsumer(
                    kafkaParams,
                    Collections.singletonList(props.getProperty("kafka.topic")),
                    props.getProperty("spark.master.server"),
                    props.getProperty("output.method"),
                    Integer.parseInt(props.getProperty("kafka.consumer.threadpool.corePoolSize")),
                    Integer.parseInt(props.getProperty("kafka.consumer.threadpool.maximumPoolSize")),
                    Integer.parseInt(props.getProperty("kafka.consumer.keep_alive_time"))
            );
            LOGGER.info("Starting kafka consumer...");

            kafkaConsumer.start();

        } catch (IOException ex) {
            LOGGER.error("Couldn't read twitter-kafka-producer-configuration.properties, exiting.");
            LOGGER.error("", ex);
            System.exit(1);
        } catch (Exception e) {
            LOGGER.error("", e);
            System.exit(2);
        }
    }
}
