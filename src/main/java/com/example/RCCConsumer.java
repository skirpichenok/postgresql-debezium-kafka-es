package com.example;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class RCCConsumer {

    // CREATE TABLE clinical_data (registertime BIGINT, id BIGINT, unique_identifier VARCHAR, event VARCHAR, repeating VARCHAR, occurence VARCHAR, name VARCHAR, version_name VARCHAR, variable_name VARCHAR, value_index VARCHAR, value VARCHAR) WITH (KAFKA_TOPIC = 'clinical_data', VALUE_FORMAT='JSON', KEY = 'id');

    static int c = 0;

    private static final String RCC_INFO_SQL = "select json_agg(t) from (" +
            "select item_data.id as item_data_id, item_data.tenant_id as tenant_id, study.name as study_name, study.id as study_id, " +
            "study_site.id as study_site_id, site.name as study_site_name, " +
            "subject.unique_identifier, subject.id as subject_id, " +
            "study_event_definition.name as event_name, study_event_definition.id as event_def_id, study_event_definition.repeating as event_def_repeating, " +
            "study_event.occurence as event_occurrence, crf_entry.id as event_crf_id, crf.name as crf_name, crf_version.version_name as crf_version_name, " +
            "item.variable_name as field_name, item_data.value_index as row, item_data_value.value as field_value " +
            "from rc_item_data item_data " +
            "join rc_item_form_metadata item_form_metadata on item_form_metadata.id = item_data.rc_item_form_metadata_id " +
            "join rc_subjects subject on subject.id = item_data.subject_id " +
            "join rc_crf_versions crf_version on crf_version.id = item_form_metadata.crf_version_id " +
            "join rc_crfs crf on crf.id = crf_version.crf_id " +
            "join rc_items item on item.id = item_form_metadata.item_id " +
            "join rc_studies study on study.id = subject.study_id " +
            "left join rc_study_sites study_site on study_site.id = subject.study_site_id " +
            "left join rc_sites site on site.id = study_site.site_id " +
            "left join rc_event_crfs crf_entry on crf_entry.id = item_data.event_crf_id " +
            "left join rc_study_events study_event on study_event.id = crf_entry.study_event_id " +
            "left join rc_study_event_definitions study_event_definition on study_event_definition.id = study_event.study_event_definition_id " +
            "left join rc_item_data_values item_data_value on item_data_value.rc_item_data_id = item_data.id " +
            "where item_data.id IN (?)) t";

    private static Producer<String, String> producer;

    private static KafkaConsumer<String, String> consumer;

    private static final JsonParser JSON_PARSER = new JsonParser();

    private static final String ES_TOPIC = "test-elasticsearch-sink";

    private static final String DB_TOPIC = "DB_TEST_SERVER.public.rc_item_data";

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(String[] args) throws Exception {
        //
        try (Connection pgConnectionTo = DriverManager.getConnection("jdbc:postgresql://localhost:5432/nphase", "postgres", "postgres")) {
            //ResultSet rs;
            int emptyC = 0;
            boolean started = false;
            boolean finished = false;
            pgConnectionTo.setAutoCommit(false);
            List<Long> ids = new ArrayList<>();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            //Kafka consumer configuration settings
            final String groupId = "group01";
            final String clientId = "client01";

            Properties producerProps = new Properties();
            producerProps.put("transactional.id","T1");
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put("schema.registry.url", "http://localhost:8081");
            producerProps.put("batch.size", "100000");
            producer = new KafkaProducer<String, String>(producerProps);
            producer.initTransactions();
            producer.beginTransaction();

            Properties consumerProps = new Properties();
            consumerProps.put("bootstrap.servers", "localhost:9092");
            consumerProps.put("group.id", groupId);
            consumerProps.put("client.id", clientId);
            consumerProps.put("enable.auto.commit", "true");
            consumerProps.put("auto.commit.interval.ms", "1000");
            consumerProps.put("session.timeout.ms", "30000");
            consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            consumer = new KafkaConsumer<String, String>(consumerProps);
            List<TopicPartition> topic = Collections.singletonList(new TopicPartition(DB_TOPIC, 0));
            consumer.assign(topic);
            consumer.seekToBeginning(topic);

            //print the topic name
            System.out.println("Subscribed to topic=" + DB_TOPIC + ", group=" + groupId + ", clientId=" + clientId);
            System.out.println("start: " + sdf.format(new Date()));

            int idx = 0;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100L);
                if (records == null) {
                    continue;
                }
                if (!records.isEmpty()) {
                    started = true;
                } else if (started) {
                    emptyC++;
                }
                if (emptyC > 50 && started && !ids.isEmpty() && !finished) {
                    finished = true;
                    processIds(pgConnectionTo, ids);
                    System.out.println("total c: " + c);
                    System.out.println("end: " + sdf.format(new Date()));
                    int z = 0;
                }
                for (ConsumerRecord<String, String> record : records) {
                    if (record == null) {
                        continue;
                    }
                    String type = "";
                    JsonObject jsonObject = JSON_PARSER.parse(record.value()).getAsJsonObject();
                    if (jsonObject == null || jsonObject.get("before") == null || jsonObject.get("after") == null) {
                        continue;
                    }
                    if (jsonObject.get("before").isJsonNull() && jsonObject.get("after").isJsonNull()) {
                        continue;
                    }
                    if (jsonObject.get("before").isJsonNull() && !jsonObject.get("after").isJsonNull()) {
                        type = "INSERT";
                    }
                    if (!jsonObject.get("before").isJsonNull() && jsonObject.get("after").isJsonNull()) {
                        type = "DELETE";
                        continue;
                    }
                    if (!jsonObject.get("before").isJsonNull() && !jsonObject.get("after").isJsonNull()) {
                        type = "UPDATE";
                        continue;
                    }
                    ids.add(Long.parseLong(jsonObject.getAsJsonObject("after").get("id").toString()));
                    //processIds(connectionTo, ps, jsonParser, ids);
                    if (ids.size() % 100000 == 0) {
                        processIds(pgConnectionTo, ids);
                    }
                    // print the offset,key and value for the consumer records.
                    // System.out.printf("offset = %d, key = %s, value = %s,  time = %s \n", record.offset(), record.key(), record.value(), sdf.format(new Date()));
                }
            }
        }
    }

    private static void processIds(Connection connectionTo, List<Long> ids) throws SQLException {
        c += ids.size();
        System.out.println(" c: " + c);
        //producer.beginTransaction();
        try (ResultSet rs = connectionTo.createStatement().executeQuery(RCC_INFO_SQL.replace("?", ids.toString().replaceAll("\\]|\\[", "")))) {
            if (rs.next()) {
                JsonElement jsonData = JSON_PARSER.parse(rs.getString(1));
                if (jsonData instanceof JsonArray) {
                    for (JsonElement jsonElement : ((JsonArray) jsonData)) {
                        producer.send(new ProducerRecord<String, String>(
                                ES_TOPIC,
                                jsonElement.getAsJsonObject().get("item_data_id").toString(),
                                jsonElement.getAsJsonObject().toString())
                        );
                    }
                }
            }
        }
        ids.clear();
        producer.flush();
        //producer.commitTransaction();
    }
}
