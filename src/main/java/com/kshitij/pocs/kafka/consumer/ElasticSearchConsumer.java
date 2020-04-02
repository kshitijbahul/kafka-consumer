package com.kshitij.pocs.kafka.consumer;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ElasticSearchConsumer {

    private ElasticSearchConsumer(){

    }

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        String hostname="localhost:9092";
        String groupName="twitter-to-elastic-consumer";
        String topic="corona_tweets_1";
        ElasticSearchConsumer esc=new ElasticSearchConsumer();
        RestHighLevelClient elasticClient=esc.createElasticClient();
        KafkaConsumer<String,String> kafkaConsumer=esc.createKafkaClient(hostname,groupName,topic);
        BulkRequest bulkRequest=new BulkRequest();
        /*Map<String,String> data= new HashMap<String,String>();
        data.put("name","Kshitij5");*/
        while(true){
            ConsumerRecords<String,String> consumerRecords=kafkaConsumer.poll(Duration.ofMillis(100));
            logger.info("Recieved "+consumerRecords.count());
            consumerRecords.forEach((consumerRecord)->{
                //Creating and Idempotent Consumer
                // 1. Create an idempotent ID
                // 2. Use something from the message
                String uniqueId= consumerRecord.topic()+"_"+consumerRecord.partition()+"_"+consumerRecord.offset();
                uniqueId=esc.getUinqueIdentifier(consumerRecord.value());
                //String id=esc.postDataToElastic(consumerRecord.value(),elasticClient,uniqueId);
                //Refactored code
                //String id=esc.postDataToElastic(esc.getIndexRequest(consumerRecord.value(),uniqueId),elasticClient);
                //logger.info("The id is "+id);
                //Commenting the above one for Bulking

                bulkRequest.add(esc.getIndexRequest(consumerRecord.value(),uniqueId));
            });
            if(consumerRecords.count()>0){
                try {
                    BulkResponse bulkResponse= elasticClient.bulk(bulkRequest,RequestOptions.DEFAULT);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                logger.info("Committing offsets ");
                kafkaConsumer.commitSync();
                logger.info("Committed Offsets");
            }

        }

        /*try {
            elasticClient.close();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Failed to close client",e);
        }*/
    }
    private String getUinqueIdentifier(String tweetJson){
        return new JsonParser().parse(tweetJson).getAsJsonObject().get("Hello").getAsString();
    }
    private KafkaConsumer<String,String> createKafkaClient(String hostname,String groupName,String topic){
        Properties consumerProperties=new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,hostname);
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupName);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //read data from beginning
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //Commit only when we want to.
        consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        consumerProperties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");//We will read only 10 messages at a time

        KafkaConsumer<String,String> kafkaConsumer=new KafkaConsumer<String, String>(consumerProperties);
        kafkaConsumer.subscribe(Arrays.asList(topic));
        return kafkaConsumer;
    }


    //private String postDataToElastic(String message,RestHighLevelClient client){
    private IndexRequest getIndexRequest(String message,String id){
        //IndexRequest indexRequest=new IndexRequest("twitter","tweets").source(message,XContentType.JSON);
        //using an Id for idempotency
        IndexRequest indexRequest=new IndexRequest("twitter","tweets",id).source(message,XContentType.JSON);
        return indexRequest;
    }
    private String postDataToElastic(IndexRequest indexRequest,RestHighLevelClient client){


        IndexResponse indexResponse= null;
        try {
            indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
            //logger.error("Index not found",e);
        }
        return indexResponse.getId();
        //logger.info("Got the ID as"+indexResponse.toString());
    }

    private RestHighLevelClient createElasticClient(){
        String username="ep6mc749qj";
        String password="uvu884e59u";
        String hostName="app-testing-kafka-8014177426.eu-central-1.bonsaisearch.net";
        final CredentialsProvider credentialsProvider= new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(username,password));
        RestClientBuilder builder= RestClient.builder(
                new HttpHost(hostName,443,"https")
                //Says connect to the host and use the creds always
        ).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });
        return new RestHighLevelClient(builder);
    }
}
