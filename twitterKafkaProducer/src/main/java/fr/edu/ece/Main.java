package fr.edu.ece;

import fr.edu.ece.twitter_consumer.TwitterConsumer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class.getName());
    private static Properties props = new Properties();
    private static TwitterConsumer tc;
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

            System.setProperty("java.security.krb5.conf", props.getProperty("kerberos.configuration.path"));
            System.setProperty("java.security.auth.login.config", props.getProperty("kerberos.configuration.jaas.path"));
            System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
            System.setProperty("sun.security.krb5.debug", "true");

            queue = new LinkedBlockingQueue<>();
            Properties kafkaProps = new Properties();
            kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("kafka.producer.bootstrap-server"));
            kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG, "twitterKafkaProducer");
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            kafkaProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, props.getProperty("kerberos.configuration.protocol"));

            LOGGER.info("Creating twitter client...");
            tc = new TwitterConsumer(
                    props,
                    kafkaProps
            );
            tc.init();
            tc.start();

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
