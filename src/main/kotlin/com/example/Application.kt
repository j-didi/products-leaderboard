package com.example

import com.example.cross.getLeaderBoard
import com.example.cross.listenToLeaderBoardEvents
import com.example.streams.ProductAggregation
import io.ktor.serialization.kotlinx.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.routing.*
import io.ktor.server.websocket.*
import kotlinx.coroutines.delay
import kotlinx.serialization.json.Json
import kotlin.time.Duration.Companion.seconds

fun main(args: Array<String>) {
    listenToLeaderBoardEvents()
    io.ktor.server.netty.EngineMain.main(args)
}

fun Application.module() {
    configureContentNegotiation()
    configureWebSocket()
    configureRouting()
}

private fun Application.configureWebSocket() {
    install(WebSockets) {
        contentConverter = KotlinxWebsocketSerializationConverter(Json)
        pingPeriod = 15.seconds
        timeout = 15.seconds
        maxFrameSize = Long.MAX_VALUE
        masking = false
    }
}

private fun Application.configureContentNegotiation() {
    install(ContentNegotiation) {
        json(Json {
            prettyPrint = true
        })
    }
}

private fun Application.configureRouting() {
    routing {


        webSocket("/tasks") {

            var lastState: List<ProductAggregation>? = null

            while (true) {

                val newState = getLeaderBoard()

                if (lastState == null || lastState != newState) {
                    sendSerialized(newState)
                }

                lastState = newState.toList()
                delay(1_000)
            }
        }
    }
}