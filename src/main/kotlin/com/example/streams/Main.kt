package com.example.streams

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {

    val producer = launch(Dispatchers.IO) { produce(Topics.SALES) }
    val aggregator = launch(Dispatchers.IO) { aggregate(Topics.SALES, Topics.AGGREGATED_SALES) }
    val consumer = launch(Dispatchers.IO) { consume(Topics.AGGREGATED_SALES) }

    joinAll(producer, aggregator, consumer)
}