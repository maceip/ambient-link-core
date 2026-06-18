package com.ambientlink.core

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL

/**
 * Polls the Ambient Link relay for the current session list.
 *
 * Canonical client shared by every Android consumer. Reads the same endpoint as
 * the glasses + web surfaces:  GET {base}/ambient-link/status
 * Response shape: { "sessions": [ { agent, cwd, state, session_id, ... } ] }
 *
 * Pick pollMs per surface: glasses run hot (~5s); Wear runs cool (~15s) to
 * protect battery. Previously duplicated in glasses + watch modules.
 */
class RelayClient(
    private val baseUrl: String,
    private val pollMs: Long = 5_000L,
) {
    private val _sessions = MutableStateFlow<List<Session>>(emptyList())
    val sessions: StateFlow<List<Session>> = _sessions.asStateFlow()

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var loop: Job? = null

    fun start() {
        if (loop?.isActive == true) return
        loop = scope.launch {
            while (isActive) {
                runCatching { fetchOnce() }.onSuccess { _sessions.value = it }
                delay(pollMs)
            }
        }
    }

    fun refreshNow() {
        scope.launch { runCatching { fetchOnce() }.onSuccess { _sessions.value = it } }
    }

    fun stop() {
        loop?.cancel()
        loop = null
    }

    private fun fetchOnce(): List<Session> {
        val url = URL("$baseUrl/ambient-link/status")
        val conn = (url.openConnection() as HttpURLConnection).apply {
            requestMethod = "GET"
            connectTimeout = 8_000
            readTimeout = 8_000
        }
        try {
            if (conn.responseCode != 200) return _sessions.value
            val body = conn.inputStream.bufferedReader().use { it.readText() }
            return parse(body)
        } finally {
            conn.disconnect()
        }
    }

    private fun parse(body: String): List<Session> {
        val arr = JSONObject(body).optJSONArray("sessions") ?: return emptyList()
        return (0 until arr.length()).map { i ->
            val o = arr.getJSONObject(i)
            Session(
                sessionId = o.optString("session_id"),
                agent = o.optString("agent", "agent"),
                cwd = o.optString("cwd"),
                state = o.optString("state", "IDLE"),
                preview = o.optString("preview", ""),
            )
        }
    }
}
