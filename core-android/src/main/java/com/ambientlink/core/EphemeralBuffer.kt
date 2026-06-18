package com.ambientlink.core

/**
 * Bounded, time-expiring buffer for captured media.
 *
 * Mirrors Cosmo's InMemoryEphemeralBuffer + getEphemeralBufferDurationMin
 * (glasses_link.md): captured frames/audio live for a bounded window and are then
 * evicted, so memory stays flat under continuous capture. Evicts on every add()
 * and snapshot(). Canonical peer of the Go backpressure.EphemeralBuffer.
 */
class EphemeralBuffer<T>(
    val ttlMillis: Long = 60_000,
    private val maxItems: Int = 64,
    private val clock: () -> Long = System::currentTimeMillis,
) {
    private data class Stamped<T>(val item: T, val ts: Long)

    private val items = ArrayDeque<Stamped<T>>()

    @Synchronized
    fun add(item: T, tsMillis: Long = clock()) {
        items.addLast(Stamped(item, tsMillis))
        while (items.size > maxItems) items.removeFirst()
        evict()
    }

    @Synchronized
    fun snapshot(): List<T> {
        evict()
        return items.map { it.item }
    }

    @Synchronized
    fun clear() = items.clear()

    private fun evict() {
        val cutoff = clock() - ttlMillis
        while (items.isNotEmpty() && items.first().ts < cutoff) items.removeFirst()
    }
}
