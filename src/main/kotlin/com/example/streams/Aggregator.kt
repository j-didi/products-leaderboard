package com.example.streams
import com.google.gson.Gson
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import java.util.*


fun aggregate(sourceTopic: String, targetTopic: String) {

    val gson = Gson()

    val productAggregationSerde = gsonSerde(gson)
    val builder = StreamsBuilder()
    val source = builder.stream<String, String>(sourceTopic)

    source
        .map { key, value -> KeyValue<Any, Any>(key, value) }
        .groupByKey()
        .count(Materialized.`as`("ProductStore"))
        .mapValues { value -> value.toString() }
        .toStream()
        .to(targetTopic)

    val streams = KafkaStreams(builder.build(), getProperties())
    streams.start()
}

fun gsonSerde(gson: Gson): Serde<Product> {
    val serializer = Serializer<Product> { _, data -> gson.toJson(data).toByteArray() }
    val deserializer = Deserializer { _, data -> gson.fromJson(String(data), Product::class.java) }
    return Serdes.serdeFrom(serializer, deserializer)
}

private fun getProperties() =
    Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, "product-aggregator-app")
        put(StreamsConfig.CLIENT_ID_CONFIG, "product-aggregator-app-client")
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java.name)
        put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String()::class.java.name)
    }