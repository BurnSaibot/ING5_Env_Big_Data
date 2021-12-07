package fr.edu.ece.kafka_producer;

import io.github.redouane59.twitter.dto.tweet.Tweet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendToKafkaTask implements Runnable{
    private static final Logger LOGGER = LoggerFactory.getLogger(SendToKafkaTask.class.getName());
    private Tweet tweet;

    public SendToKafkaTask (Tweet tweet) {
        this.tweet = tweet;
    }

    @Override
    public void run() {
        LOGGER.info("WIP - read tweet {Author: {} - Content: {}}",tweet.getAuthorId(), tweet.getText());
    }
}
