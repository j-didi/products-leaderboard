package com.example.cross

import com.example.streams.ProductAggregation
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPubSub

private val jedis = Jedis("localhost", 6379)
private var leaderBoard: List<ProductAggregation>? = null

fun save(aggregations: List<ProductAggregation>) {
    jedis.pipelined().use { pipeline ->
        aggregations.forEach { pipeline.incrBy(it.name, it.quantity.toLong()) }
        pipeline.sync()
    }
}

fun getLeaderBoard(shouldUpdate: Boolean = true): List<ProductAggregation> {

    if (leaderBoard != null && !shouldUpdate) {
        return leaderBoard!!
    }

    val keys = jedis.keys("*").map { it }.toTypedArray()
    val values = jedis.mget(*keys)
    leaderBoard = keys
        .zip(values)
        .map { (key, value) -> ProductAggregation(key, value.toInt()) }
        .sortedByDescending { it.quantity }
        .toList()

    return leaderBoard!!
}

fun listenToLeaderBoardEvents() {
    val jedisPubSub = object : JedisPubSub() {
        override fun onPMessage(pattern: String, channel: String, message: String) {
            when (message) {
                "incrby" -> leaderBoard = getLeaderBoard(true)
            }
        }
    }

    Thread { Jedis("localhost", 6379).psubscribe(jedisPubSub, "__keyspace@0__:*") }.start()
}