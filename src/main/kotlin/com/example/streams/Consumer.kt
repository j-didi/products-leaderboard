package com.example.streams

import com.example.cross.save
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.*

fun consume(topic: String) {
    KafkaConsumer<String, String>(getProperties()).use { consumer ->
        consumer.subscribe(listOf(topic))
        while (true) {
            val records = consumer.poll(Duration.ofMillis(1000))
            val items: MutableList<ProductAggregation> = mutableListOf()

            for (record in records) {
                items.add(ProductAggregation(record.key(), record.value().toInt()))
            }

            if (items.size > 0) {
                save(items)
            }

        }
    }
}

private fun getProperties() =
    Properties().apply {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
        put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group")
    }