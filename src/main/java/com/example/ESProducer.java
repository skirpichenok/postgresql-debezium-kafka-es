package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Deprecated
public class ESProducer {

    private static final String TOPIC = "test-elasticsearch-sink";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // hardcoding the Kafka server URI for this example
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put("schema.registry.url", "http://localhost:8081");
        //producerProps.put(ProducerConfig.ACKS_CONFIG, "1");

        Producer<String, String> producer = new KafkaProducer<String, String>(producerProps);

        // Using IP as key, so events from same IP will go to same partition
        //ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, "1", "{\"name\": \"field1\", \"value\": \"bbbbbb1\"}");

        producer.send(new ProducerRecord<String, String>(TOPIC, "1", "{\"name\": \"field1\", \"value\": \"www\"}")).get();

        producer.send(new ProducerRecord<String, String>(TOPIC, "2", "{\"name\": \"field1\", \"value\": \"vv\"}")).get();
    }
}
