package com.example.streams

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import java.lang.Thread.sleep
import java.util.*

fun produce(topic: String) {
    var counter = 1
    KafkaProducer<String, String>(getProperties()).use { producer ->
        while (true) {
            val product = generateProduct()
            val record = ProducerRecord(topic, product.name, product.toJson())
            producer.send(record)
            println("Message $counter successfully!")
            ++counter
            sleep(1000)
        }
    }
}

private fun getProperties() =
    Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    }

private fun generateProduct() =
    Product(
        name = listOf("iPhone", "Samsung", "Xiaomi", "Motorola").random(),
        quantity = 1
    )