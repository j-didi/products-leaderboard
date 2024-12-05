package com.example.streams

data class Product(
    val name: String,
    val quantity: Int,
) {
    fun toJson(): String {
        return """
            {
                "name": "$name",
                "quantity": $quantity
            }
        """.trimIndent()
    }
}
