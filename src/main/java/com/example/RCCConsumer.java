package com.example;

import com.google.gson.JsonParser;
import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import com.jsoniter.output.JsonStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class RCCConsumer {

    private static final String RCC_ITEMS_SQL = "select concat(i.variable_name, '_', i.study_id, '_', ifm.item_data_type_id) " +
            "from rc_item_form_metadata ifm "+
            "join rc_items i on i.id = ifm.item_id "+
            "where  i.variable_name notnull and i.variable_name !='' and i.variable_name ~* '^[A-Z][A-Z0-9_]+$'";

    private static final String RCC_INFO_SQL = "select json_agg(t) from (" +
            "select item_data.id as item_data_id, item_form_metadata.item_data_type_id as item_data_type_id, " +
            "item_data.tenant_id as tenant_id, study.name as study_name, study.id as study_id, study_site.id as study_site_id, " +
            "site.name as study_site_name, subject.unique_identifier as subject_name, subject.id as subject_id, study_event.id as study_event_id, " +
            "study_event_definition.name as event_name, study_event_definition.id as event_def_id, study_event_definition.repeating as event_def_repeating, " +
            "(case when study_event.repeating_form_parent_id isnull then study_event.occurence else parent_study_event.occurence end)as event_occurrence, " +
            "(case when event_definition_crf.repeating then (case when study_event.repeating_form_parent_id isnull then 1 else study_event.occurence end) else null end) as crf_occurrence, " +
            "crf.name as crf_name, " +
            //"crf_entry.id as event_crf_id, " +
            //"(case when crf_version.version_name isnull or crf_version.version_name = '' then 'original' else crf_version.version_name end) as crf_version_name, " +
            "item_group_type.lookup_code as item_group_type, item_data.value_index as repeating_group_number,  " +
            "item.variable_name as field_name, item_data_value.value as field_value " +
            "from rc_item_data item_data " +
            "join rc_item_form_metadata item_form_metadata on item_form_metadata.id = item_data.rc_item_form_metadata_id " +
            "join rc_subjects subject on subject.id = item_data.subject_id " +
            "left join rc_crf_versions crf_version on crf_version.id = item_form_metadata.crf_version_id " +
            "left join rc_crfs crf on crf.id = crf_version.crf_id " +
            "join rc_items item on item.id = item_form_metadata.item_id " +
            "join rc_studies study on study.id = subject.study_id " +
            "left join rc_study_sites study_site on study_site.id = subject.study_site_id " +
            "left join rc_sites site on site.id = study_site.site_id " +
            "left join rc_event_crfs crf_entry on crf_entry.id = item_data.event_crf_id " +
            "left join rc_event_definition_crfs event_definition_crf on event_definition_crf.id = crf_entry.event_definition_crf_id " +
            "left join rc_study_events study_event on study_event.id = crf_entry.study_event_id " +
            "left join rc_study_events parent_study_event on parent_study_event.id = study_event.repeating_form_parent_id " +
            "left join rc_study_event_definitions study_event_definition on study_event_definition.id = study_event.study_event_definition_id " +
            "left join rc_item_data_values item_data_value on item_data_value.rc_item_data_id = item_data.id " +
            "left join rc_item_groups item_group on item_group.id = item_form_metadata.group_id " +
            "left join ad_lookup_codes item_group_type on item_group_type.id = item_group.id " +
            "where item.variable_name ~* '^[A-Z][A-Z0-9_]+$' and item_data.id IN (?)) t";

    private static int globalCount = 0;

    private static final String EMPTY = "";

    private static HTreeMap<String, String> cache;

    private static KafkaProducer<String, String> producer;

    private static KafkaConsumer<String, String> consumer;

    private static final JsonParser JSON_PARSER = new JsonParser();

    private static final String ES_TOPIC = "test-elasticsearch-sink";

    private static final String DB_TOPIC = "DB_TEST_SERVER.public.rc_item_data";

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(String[] args) throws Exception {
        try (DB dbDisk = DBMaker.fileDB("rcc_consumer_cache_".concat(Long.toString(System.currentTimeMillis())).concat(".tmp")).make();
             Connection pgConnectionTo = DriverManager.getConnection("jdbc:postgresql://localhost:5432/nphase", "postgres", "postgres")) {
            int emptyC = 0;
            boolean started = false;
            boolean finished = false;
            List<Long> ids = new ArrayList<>();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            cache = dbDisk.hashMap("onDisk", Serializer.STRING, Serializer.STRING).create();

            Properties producerProps = new Properties();
            producerProps.put("transactional.id","T1");
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put("schema.registry.url", "http://localhost:8081");
            producerProps.put("batch.size", "100000");

            producer = new KafkaProducer<>(producerProps);
            producer.initTransactions();

            Properties consumerProps = new Properties();
            consumerProps.put("bootstrap.servers", "localhost:9092");
            consumerProps.put("group.id", "group01");
            consumerProps.put("client.id","client01");
            consumerProps.put("enable.auto.commit", "true");
            consumerProps.put("auto.commit.interval.ms", "1000");
            consumerProps.put("session.timeout.ms", "30000");
            consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            List<TopicPartition> topic = Collections.singletonList(new TopicPartition(DB_TOPIC, 0));
            consumer = new KafkaConsumer<>(consumerProps);
            consumer.assign(topic);
            consumer.seekToBeginning(topic);

            System.out.println("start: " + sdf.format(new Date()));

            //22 371 119
            //start: 2019-08-05 16:28:11
            //end: 2019-08-05 16:41:01
            //es end: 16:48
            //308 270 indexes
            //~30 mins

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
                    /*
                    producer.beginTransaction();
                    for (String indexId : cache.getKeys()) {
                        producer.send(new ProducerRecord<>(
                                ES_TOPIC,
                                indexId,
                                cache.get(indexId))
                        );
                    }
                    producer.flush();
                    producer.commitTransaction();
                    */
                    System.out.println("cache size: " + cache.size());
                    System.out.println("total count: " + globalCount);
                    System.out.println("end: " + sdf.format(new Date()));
                }
                for (ConsumerRecord<String, String> record : records) {
                    if (record == null) {
                        continue;
                    }
                    Any jsonObject = JsonIterator.deserialize(record.value());
                    if (jsonObject == null) {
                        continue;
                    }
                    if (jsonObject.get("before").as(Any.class) == null && jsonObject.get("after").as(Any.class) == null ) {
                        continue;
                    }
                    if (jsonObject.get("before").as(Any.class) == null  && jsonObject.get("after").as(Any.class) != null ) {
                        // INSERT
                    }
                    if (jsonObject.get("before").as(Any.class) != null  && jsonObject.get("after").as(Any.class) == null ) {
                        // DELETE
                        continue;
                    }
                    if (jsonObject.get("before").as(Any.class) != null  && jsonObject.get("after").as(Any.class) != null ) {
                        // UPDATE
                        continue;
                    }
                    ids.add(Long.parseLong(jsonObject.get("after").get("id").toString()));
                    if (ids.size() % 100000 == 0) {
                        processIds(pgConnectionTo, ids);
                    }
                }
            }
        }
    }

    private static void processIds(Connection connectionTo, List<Long> ids) throws SQLException {
        globalCount += ids.size();
        producer.beginTransaction();
        System.out.println("count: " + globalCount);
        try (ResultSet rs = connectionTo.createStatement().executeQuery(RCC_INFO_SQL.replace("?", ids.toString().replaceAll("\\]|\\[", EMPTY)))) {
            if (rs.next()) {
                Any jsonData = JsonIterator.deserialize(rs.getString(1));
                for (final Any jsonElement : jsonData) {
                    // build index id
                    String indexId = jsonElement.get("subject_id").toString().concat("_").concat(jsonElement.get("study_event_id").toString());

                    Any value = jsonElement.get("field_value");
                    String fieldName = jsonElement.get("field_name").toString().
                            concat("_").concat(jsonElement.get("study_id").toString()).
                            concat("_").concat(jsonElement.get("item_data_type_id").toString());
                    String fieldValue = value.as(Any.class) != null ? value.toString() : EMPTY;

                    Map<String, Any>  map = jsonElement.asMap();
                    map.remove("field_name");
                    map.remove("field_value");

                    String jsonString = cache.computeIfAbsent(indexId, v -> jsonElement.toString());
                    Any jsonElementData = JsonIterator.deserialize(jsonString);
                    jsonElementData.asMap().put(fieldName, Any.wrap(JsonStream.serialize(fieldValue)));

                    cache.put(indexId, jsonElementData.toString());

                    producer.send(new ProducerRecord<>(
                            ES_TOPIC,
                            indexId,
                            jsonElementData.toString())
                    );
                }
            }
        }
        ids.clear();
        producer.flush();
        producer.commitTransaction();
    }
}
