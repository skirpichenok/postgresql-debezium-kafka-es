package com.example;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class DpiMqtt<R extends ConnectRecord<R>> implements Transformation<R> {

    @Override
    public R apply(R record) {
        //Key looks something like this "unibuss/ruter/101025/itxpt/ota/dpi/journey/json"
        String key = (String) record.key();
        String[] keys = key.split("/");
        String operator = keys[0];
        String vehicleId = keys[2];
        Gson gson = new Gson();
        JsonObject recordValue = gson.toJsonTree(record.value()).getAsJsonObject();

        recordValue.addProperty("operator", operator);
        recordValue.addProperty("vehicleId", vehicleId);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                null,
                recordValue,
                record.timestamp()
        );
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
        //
    }

    @Override
    public void configure(Map<String, ?> configs) {
        //
    }
}
