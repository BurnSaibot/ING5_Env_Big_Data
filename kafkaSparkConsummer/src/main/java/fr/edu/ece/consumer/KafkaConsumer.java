package fr.edu.ece.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class KafkaConsumer{
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);
    private JavaSparkContext javaSparkContext;
    private JavaStreamingContext javaStreamingContext;
    private JavaInputDStream<ConsumerRecord<String, String>> stream;
    private Boolean stop;
    private String outputMethod;
    private ThreadPoolExecutor threadPoolExecutor;


    public KafkaConsumer(Map<String,Object> kafkaParams, Collection<String> topics, String sparkMaster, String outputMethod, int corePoolSize, int maxPoolSize, int keepAliveTime)  {
        LOGGER.info("Initializing kafka consumer and spark streaming");
        SparkConf sparkConf = new SparkConf().setAppName("kafka-consumer-for-tweet-streaming").setMaster(sparkMaster);
        this.javaSparkContext = new JavaSparkContext(sparkConf);
        //Data read every second
        this.javaStreamingContext = new JavaStreamingContext(javaSparkContext, new Duration(1000));
        this.stream =
                KafkaUtils.createDirectStream(
                        javaStreamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );
        this.stop = false;
        this.outputMethod = outputMethod;
        this.threadPoolExecutor = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                keepAliveTime,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>()
        );
    }

    public void start() {
        stop = Boolean.FALSE;
        stream.foreachRDD(rdd -> {

            if (rdd.count() > 0) {
                rdd.collect().forEach(rawRecord -> {
                    LOGGER.info("TOTO Record: {}", rawRecord);
                });
            } else {
                LOGGER.info("No tweets to display");
            }
        });
        javaStreamingContext.start();
    }

    public void stop() {
        try {
            javaStreamingContext.awaitTermination();
        } catch (InterruptedException e) {
            LOGGER.error("", e);
            javaStreamingContext.stop();
        }
        stop = Boolean.TRUE;
    }

    public Boolean isStopped() {
        return stop;
    }
}
