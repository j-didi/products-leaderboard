package com.example.streams

import kotlinx.serialization.Serializable

@Serializable
data class ProductAggregation(
    val name: String,
    val quantity: Int
)