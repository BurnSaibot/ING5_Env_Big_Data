package fr.edu.ece.kafka_producer;

import io.github.redouane59.twitter.dto.tweet.Tweet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class KafkaProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class.getName());
    private ThreadPoolExecutor executor;

    public KafkaProducer (int coreSize, int maximumSize, int keepAlive, BlockingQueue<Runnable> queue) {
        executor = new ThreadPoolExecutor(coreSize, maximumSize, keepAlive, TimeUnit.SECONDS, queue);
    }

    public void sendToKafka(SendToKafkaTask sendTweetTask) {
        executor.execute(sendTweetTask);
    }
}
