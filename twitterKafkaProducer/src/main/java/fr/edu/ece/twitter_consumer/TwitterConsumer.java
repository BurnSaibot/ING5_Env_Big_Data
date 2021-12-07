package fr.edu.ece.twitter_consumer;

import com.github.scribejava.core.model.Response;
import fr.edu.ece.kafka_producer.KafkaProducer;
import fr.edu.ece.kafka_producer.SendToKafkaTask;
import io.github.redouane59.twitter.TwitterClient;
import io.github.redouane59.twitter.dto.rules.FilteredStreamRulePredicate;
import io.github.redouane59.twitter.signature.TwitterCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.Future;

public class TwitterConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterConsumer.class.getClass());
    private TwitterClient client;
    private String [] hashtagsToFollow;
    private ArrayList<FilteredStreamRulePredicate> rules;
    private Future<Response> stream;
    private KafkaProducer kafkaProducer;

    public TwitterConsumer(String apiKey, String apiKeySecret, String apiToken, String apiTokenSecret, String [] hashtagsToFollow, KafkaProducer producer) {
        LOGGER.info("Api: apiKey: {}, apiKeySecret: {}, apiToken: {}, apiTokenSecret: {}", apiKey, apiKeySecret, apiToken, apiTokenSecret);
        client = new TwitterClient(TwitterCredentials.builder()
                .apiKey(apiKey)
                .apiSecretKey(apiKeySecret)
                .accessToken(apiToken)
                .accessTokenSecret(apiTokenSecret)
                .build());
        this.hashtagsToFollow = hashtagsToFollow.clone();
        this.rules = new ArrayList<>();
        this.kafkaProducer = producer;
    }

    public void init() throws IllegalArgumentException {
        LOGGER.info("Adding shutdown hook...");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Running shutdown hook...");
            for (FilteredStreamRulePredicate rule : rules) {
                client.deleteFilteredStreamRule(rule);
            }
        }));

        ArrayList<FilteredStreamRulePredicate> rulesBeforeMerging = new ArrayList<>();
        for (String toFollow : hashtagsToFollow) {
            FilteredStreamRulePredicate r = FilteredStreamRulePredicate.withHashtag(toFollow);
            rulesBeforeMerging.add(r);
        }

        switch (rulesBeforeMerging.size()) {
            case 0 :
                throw new IllegalArgumentException("TwitterConsumer failed to init as no Hashtag were given, please add one to twitter-api-configuration.");
            case 1 :
                rules.add(rulesBeforeMerging.get(0));
                    client.addFilteredStreamRule(rulesBeforeMerging.get(0), "hashtags");
                break;
            default :
                FilteredStreamRulePredicate mergedRule = mergeHashtag(rulesBeforeMerging);
                rules.add(mergedRule);
                client.addFilteredStreamRule(mergedRule, "hashtags");
        }
    }

    public void start () {
        if (stream == null) {
            stream = client.startFilteredStream((tweet) -> {
                kafkaProducer.sendToKafka(new SendToKafkaTask(tweet));
            });
        } else {
            LOGGER.warn("TwitterConsumer.start() got called but consumer was alredy started !! - Nothing was done");
        }

    }

    public void stop () {
        if (stream != null) {
            client.stopFilteredStream(stream);
            for (FilteredStreamRulePredicate rule : rules) {
                LOGGER.error("Removing rule from filtered stream {}",rule);
                client.deleteFilteredStreamRule(rule);
            }
            rules.clear();
        } else {
            LOGGER.warn("TwitterConsumer.stop() got called as it was not started !! - Nothing was done");
        }

    }

    private FilteredStreamRulePredicate mergeHashtag (ArrayList<FilteredStreamRulePredicate> rules) {
        FilteredStreamRulePredicate mergedRule = rules.get(0);
        for (int i = 1; i < rules.size(); i++) {
            mergedRule = mergedRule.or(rules.get(i));
        }
        return mergedRule;
    }
}
