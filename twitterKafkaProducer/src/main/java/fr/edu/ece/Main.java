package fr.edu.ece;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.edu.ece.kafka_producer.KafkaProducer;
import fr.edu.ece.kafka_producer.SendToKafkaTask;
import fr.edu.ece.twitter_consumer.TwitterConsumer;
import io.github.redouane59.twitter.TwitterClient;
import io.github.redouane59.twitter.dto.rules.FilteredStreamRulePredicate;
import io.github.redouane59.twitter.signature.TwitterCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class.getName());
    private static Properties props = new Properties();
    private static TwitterConsumer tc;
    private static KafkaProducer kafkaProducer;
    private static BlockingQueue<Runnable> queue;

    private Main() {

    }

    public static void main (String[] args) {
        if ( System.getProperty("twitter.kafka_producer.configuration") == null) {
            LOGGER.error("Please use option -Dtwitter.kafka_producer.configuration to provide a path to your twitter api config file. Exiting.");
            System.exit(1);
        }
        LOGGER.info("configuration file path: {}", System.getProperty("twitter.kafka_producer.configuration"));
        try (InputStream input = new FileInputStream(new File(System.getProperty("twitter.kafka_producer.configuration")))) {

            // load a properties file
            LOGGER.info("Parsing configuration file...");
            props.load(input);

            queue = new LinkedBlockingQueue<>();
            kafkaProducer = new KafkaProducer(
                    Integer.parseInt(props.getProperty("kafka.producer.threadpool.corePoolSize")),
                    Integer.parseInt(props.getProperty("kafka.producer.threadpool.maximumPoolSize")),
                    Integer.parseInt(props.getProperty("kafka.producer.keep_alive_time")),
                    queue
            );

            LOGGER.info("Creating twitter client...");
            String [] hashtagsToFollow = props.getProperty("twitter.api.hashtags_to_follow").split("\\|");
            tc = new TwitterConsumer(

                    props.getProperty("twitter.api.key"),
                    props.getProperty("twitter.api.key.secret"),
                    props.getProperty("twitter.api.access_token"),
                    props.getProperty("twitter.api.acces_token.secret"),
                    hashtagsToFollow,
                    kafkaProducer
            );
            tc.init();
            tc.start();

        } catch (IOException ex) {
            LOGGER.error("Couldn't read twitter-api-configuration.properties, exiting.");
            LOGGER.error("", ex);
            System.exit(1);
        } catch (Exception e) {
            LOGGER.error("", e);
            System.exit(2);
        }
    }
}
