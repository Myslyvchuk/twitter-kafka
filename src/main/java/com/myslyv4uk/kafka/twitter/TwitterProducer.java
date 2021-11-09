package com.myslyv4uk.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TwitterProducer {

    private final String consumerKey = "";
    private final String consumerSecret = "";
    private final String token = "-";
    private final String secret = "";

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        //twitter client
        new TwitterProducer().run();
    }

    private void run() {
        log.info("Set up!");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
        Client client = createTwitterClient(msgQueue);
        client.connect();

        //KafkaProducer<String, String> producer =


        while(!client.isDone()){
            String msg = null;
            try {
                 msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                log.info("{}",msg);
            }
        }
        log.info("End of app!");
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts twitterKafkaHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint tiwtterKafkaEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("bitcoin");
        tiwtterKafkaEndpoint.trackTerms(terms);

        Authentication twitterKafkaAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Kafka-Twitter-Client-01")                      // optional: mainly for the logs
                .hosts(twitterKafkaHosts)
                .authentication(twitterKafkaAuth)
                .endpoint(tiwtterKafkaEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));  // optional: use this if you want to process client events

        return builder.build();
    }
}
