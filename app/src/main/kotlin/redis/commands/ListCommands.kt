package redis.commands

import redis.models.LISTS
import redis.protocol.NULL_ARRAY
import redis.protocol.writeOutput
import java.io.OutputStream
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

fun popLeft(entry: LISTS, blocking: Boolean, timeoutMs: Long? = null): String? {
    entry.lock.lock()
    try {
        if (!blocking) {
            return if (entry.redisList.isEmpty()) null else entry.redisList.removeAt(0)
        }

        if (timeoutMs == null || timeoutMs == 0L) {
            while (entry.redisList.isEmpty()) {
                entry.notEmpty.await()
            }
            return entry.redisList.removeAt(0)
        }

        var nanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs)
        while (entry.redisList.isEmpty()) {
            if (nanos <= 0L) return null
            nanos = entry.notEmpty.awaitNanos(nanos)
        }

        return entry.redisList.removeAt(0)
    } finally {
        entry.lock.unlock()
    }
}

fun rPUSH(
    output: OutputStream,
    parts: MutableList<String>,
    listContainer: ConcurrentHashMap<String, LISTS>
) {
    val key = parts.getOrNull(1) ?: ""
    val values = parts.drop(2)
    val entry = listContainer.getOrPut(key) { LISTS() }

    entry.lock.lock()
    val listLen: Int
    try {
        entry.redisList.addAll(values)
        entry.notEmpty.signalAll()
        listLen = entry.redisList.size
    } finally {
        entry.lock.unlock()
    }

    writeOutput(output, ":$listLen\r\n")
}

fun lPUSH(
    output: OutputStream,
    parts: MutableList<String>,
    listContainer: ConcurrentHashMap<String, LISTS>
) {
    val key = parts.getOrNull(1) ?: ""
    val values = parts.drop(2)
    val entry = listContainer.getOrPut(key) { LISTS() }

    entry.lock.lock()
    val listLen: Int
    try {
        entry.redisList.addAll(0, values.reversed())
        entry.notEmpty.signalAll()
        listLen = entry.redisList.size
    } finally {
        entry.lock.unlock()
    }

    writeOutput(output, ":$listLen\r\n")
}

fun lLEN(
    output: OutputStream,
    parts: MutableList<String>,
    listContainer: ConcurrentHashMap<String, LISTS>
) {
    val key = parts.getOrNull(1) ?: ""
    val entry = listContainer.getOrPut(key) { LISTS() }
    writeOutput(output, ":${entry.redisList.size}\r\n")
}

fun lRANGE_POS(
    output: OutputStream,
    parts: MutableList<String>,
    listContainer: ConcurrentHashMap<String, LISTS>
) {
    val key = parts.getOrNull(1) ?: ""
    val startIdx = parts.getOrNull(2)!!.toInt()
    val stopIdx = parts.getOrNull(3)!!.toInt()
    val list = listContainer[key]?.redisList ?: mutableListOf()

    if (list.isEmpty() || startIdx > stopIdx || startIdx >= list.size) {
        writeOutput(output, "*0\r\n")
        return
    }

    val safeStart = startIdx.coerceAtLeast(0)
    val safeStop = stopIdx.coerceAtMost(list.size - 1)
    val slice = list.subList(safeStart, safeStop + 1)

    val response = buildString {
        append("*${slice.size}\r\n")
        slice.forEach { append("\$${it.length}\r\n$it\r\n") }
    }
    writeOutput(output, response)
}

fun lRANGE_NEG(
    output: OutputStream,
    parts: MutableList<String>,
    listContainer: ConcurrentHashMap<String, LISTS>
) {
    val key = parts.getOrNull(1) ?: ""
    val list = listContainer[key]!!.redisList
    val len = list.size

    val startIdx = parts.getOrNull(2)!!.toInt().let { if (it < 0) len + it else it }
    val stopIdx = parts.getOrNull(3)!!.toInt().let { if (it < 0) len + it else it }

    if (list.isEmpty() || startIdx > stopIdx || startIdx >= list.size) {
        writeOutput(output, "*0\r\n")
        return
    }

    val safeStart = startIdx.coerceAtLeast(0)
    val safeStop = stopIdx.coerceAtMost(list.size - 1)
    val slice = list.subList(safeStart, safeStop + 1)

    val response = buildString {
        append("*${slice.size}\r\n")
        slice.forEach { append("\$${it.length}\r\n$it\r\n") }
    }
    writeOutput(output, response)
}

fun lPOP(
    output: OutputStream,
    parts: MutableList<String>,
    listContainer: ConcurrentHashMap<String, LISTS>
) {
    val key = parts.getOrNull(1) ?: ""
    val entry = listContainer.getOrPut(key) { LISTS() }
    val value = popLeft(entry, blocking = false)

    val response = if (value == null) "\$-1\r\n" else "\$${value.length}\r\n$value\r\n"
    writeOutput(output, response)
}

fun lPOP_RANGE(
    output: OutputStream,
    parts: MutableList<String>,
    listContainer: ConcurrentHashMap<String, LISTS>
) {
    val key = parts.getOrNull(1) ?: ""
    val count = parts.getOrNull(2)?.toIntOrNull()
    if (count == null || count <= 0) {
        writeOutput(output, "-ERR value is not an integer or out of range\r\n")
        return
    }

    val list = listContainer[key]?.redisList
    if (list.isNullOrEmpty()) {
        writeOutput(output, "*0\r\n")
        return
    }

    val take = minOf(count, list.size)
    val response = buildString {
        append("*$take\r\n")
        for (i in 0 until take) append("\$${list[i].length}\r\n${list[i]}\r\n")
    }
    list.subList(0, take).clear()

    writeOutput(output, response)
}

fun bLPOP(
    output: OutputStream,
    parts: MutableList<String>,
    listContainer: ConcurrentHashMap<String, LISTS>
) {
    val key = parts.getOrNull(1) ?: ""
    val timeoutMs = ((parts.getOrNull(2)?.toDoubleOrNull() ?: 0.0) * 1_000).toLong()
    val entry = listContainer.getOrPut(key) { LISTS() }
    val value = popLeft(entry, blocking = true, timeoutMs = timeoutMs)

    val response = if (value == null) {
        NULL_ARRAY
    } else {
        "*2\r\n\$${key.length}\r\n$key\r\n\$${value.length}\r\n$value\r\n"
    }
    writeOutput(output, response)
}
