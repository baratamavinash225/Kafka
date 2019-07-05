package com.pk.kafka.streams.iot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.KafkaStreams;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;
import org.apache.kafka.streams.KeyValue;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;

public class IotStreamsStarterApp {

    public static KeyValue<String, String> returnKeyValuePair(String rawValue) {

        String value = rawValue.substring(rawValue.indexOf("{"), rawValue.length()).trim();
        KeyValue<String, String> kv = null;
        try {
            Object obj = new JSONParser().parse(value);
            JSONObject jo = (JSONObject) obj;
            kv = KeyValue.pair((String)((String)jo.get("metricName")+"::"+(String)jo.get("assetName")), (String)jo.get("metricVal"));
            System.out.println(kv.key);
            System.out.println(kv.value);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return kv;

    }


    public static JsonNode newSumValue(String key, String metric_value, JsonNode cpu_sum) {
        // create a new sum json object
        ObjectNode new_cpu_sum = JsonNodeFactory.instance.objectNode();
        new_cpu_sum.put("key", key);
        new_cpu_sum.put("keyCounts", cpu_sum.get("keyCounts").asInt() + 1);
        new_cpu_sum.put("cpu_sum", cpu_sum.get("cpu_sum").floatValue() + Float.parseFloat(metric_value));
        return new_cpu_sum;
    }

    public static JsonNode keySumInitialValue() {
        ObjectNode initialValue = JsonNodeFactory.instance.objectNode();
        initialValue.put("key", "");
        initialValue.put("keyCounts", 0);
        initialValue.put("cpu_sum", 0F);
            return initialValue;
    }
    public static void main(String[] args) {

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "iot-streams-starter-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> cpuMetricInput = builder.stream("cpu_usage_metric");
        //System.out.println("Json String"+cpuMetricInput.toString());






        KTable<String, JsonNode> cpu_metric_value = cpuMetricInput.map((ignoreKey, value) -> returnKeyValuePair(value))
                                                 .groupByKey()
                                                 .aggregate(
                                                         () -> keySumInitialValue(),
                                                         (key, metric_value, cpu_sum) -> newSumValue(key, metric_value, cpu_sum),
                                                         jsonSerde,
                                                         "sum-of-metric-values"
                                                 );

        cpu_metric_value.to(Serdes.String(), jsonSerde,"cpu_metric_sum");
        cpu_metric_value.mapValues(value -> (Float)(value.get("cpu_sum").floatValue()/value.get("keyCounts").asInt()))
                        .to(Serdes.String(), Serdes.Float(),"cpu_metric_average");

        KafkaStreams streams = new KafkaStreams(builder, config);

        streams.start();

        //print the topology
        //System.out.println("Stream Information" + streams.toString());

        //Shut down the application
       // Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
