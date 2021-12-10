package fr.edu.ece.twitter_consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.redouane59.twitter.dto.tweet.Tweet;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SendTweetToKafkaTask implements Runnable {
    private final static Logger LOGGER = LoggerFactory.getLogger(SendTweetToKafkaTask.class.getName());
    private final String TOPIC;
    private String [] hashtagsToFollow;
    private Tweet tweet;
    private  Producer<String, String> producer;

    public SendTweetToKafkaTask (Tweet tweet, String [] hashtagsToFollow, String topic, Producer<String, String> producer) {
        this.TOPIC = topic;
        // we will just read from this. Don't need to clone.
        this.hashtagsToFollow = hashtagsToFollow;
        this.tweet = tweet;
        this.producer = producer;
    }
    @Override
    public void run() {
        // first we have to check what # did match our rule
        String text = tweet.getText();
        ArrayList<String> hashtagsInTweet = new ArrayList<>();
        Matcher hashtagMatcher = Pattern.compile("#\\w+").matcher(text);
        LOGGER.info("Content: {}", text);
        while (hashtagMatcher.find()) {
            LOGGER.info("found {}", hashtagMatcher.group());
            hashtagsInTweet.add(hashtagMatcher.group());
        }
        LOGGER.info("list of hashtags {}", hashtagsInTweet);
        ArrayList<String> retainedHashtags = new ArrayList<>();
        for (String hashtag : hashtagsInTweet) {
            for (int i = 0; i < hashtagsToFollow.length; i++) {
                if (hashtag.equals(hashtagsToFollow[i]) || hashtag.toLowerCase().equals(hashtagsToFollow[i])) {
                    retainedHashtags.add(hashtag);
                }
            }
        }

        // for each # matched, we send the tweet (duplicated for each # to be able to aggregate without losing any data
        ObjectMapper mapper = new ObjectMapper();
        for (String matched : hashtagsInTweet) {

            ObjectNode toSend = mapper.createObjectNode();
            toSend.put("Hashtag", matched);
            toSend.put("Author", tweet.getAuthorId());
            toSend.put("Content", tweet.getText());
            try {
                sendTweet(mapper.writeValueAsString(toSend));
            } catch (JsonProcessingException e) {
                LOGGER.error("", e);
            }
        }

    }

    private void sendTweet(String payload) {
        final ProducerRecord<String, String> recordTweet = new ProducerRecord<>(TOPIC, tweet.getId(), payload);
        producer.send(recordTweet, (metadata, exception) -> {
            if (metadata != null) {
                LOGGER.info(
                        "Sent tweet {} to partition {} with offset {}",
                        recordTweet.key(),
                        metadata.partition(),
                        metadata.offset()
                );
            } else {
                LOGGER.error("", exception);
            }
        });
    }
}
