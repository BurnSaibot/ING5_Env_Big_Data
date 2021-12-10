package fr.edu.ece.twitter_consumer;

import com.github.scribejava.core.model.Response;
import io.github.redouane59.twitter.TwitterClient;
import io.github.redouane59.twitter.dto.rules.FilteredStreamRulePredicate;
import io.github.redouane59.twitter.signature.TwitterCredentials;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TwitterConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterConsumer.class.getClass());
    private final String TOPIC;

    private TwitterClient client;
    private String [] hashtagsToFollow;
    private ArrayList<FilteredStreamRulePredicate> rules;
    private Future<Response> stream;
    private Producer<String, String> producer;
    private ThreadPoolExecutor executor;


    public TwitterConsumer(Properties props, Properties kafkaProps) {
        client = new TwitterClient(TwitterCredentials.builder()
                .apiKey(props.getProperty("twitter.api.key"))
                .apiSecretKey(props.getProperty("twitter.api.key.secret"))
                .accessToken(props.getProperty("twitter.api.access_token"))
                .accessTokenSecret(props.getProperty("twitter.api.acces_token.secret"))
                .build());
        this.executor = new ThreadPoolExecutor(
                Integer.parseInt(props.getProperty("kafka.producer.threadpool.corePoolSize")),
                Integer.parseInt(props.getProperty("kafka.producer.threadpool.maximumPoolSize")),
                Integer.parseInt(props.getProperty("kafka.producer.keep_alive_time")),
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>()
        );
        this.hashtagsToFollow = props.getProperty("twitter.api.hashtags_to_follow").split("\\|");


        this.producer = new KafkaProducer<>(kafkaProps);
        this.rules = new ArrayList<>();
        this.TOPIC = props.getProperty("kafka.producer.topic");
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
                executor.execute(new SendTweetToKafkaTask(tweet, hashtagsToFollow, TOPIC, producer));
            });
        } else {
            LOGGER.warn("TwitterConsumer.start() got called but consumer was already started !! - Nothing was done");
        }

    }

    public void stop () {
        if (stream != null) {
            client.stopFilteredStream(stream);
            for (FilteredStreamRulePredicate rule : rules) {
                LOGGER.error("Removing rule from filtered stream {}", rule);
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
