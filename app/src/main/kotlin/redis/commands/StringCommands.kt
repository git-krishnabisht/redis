package redis.commands

import redis.models.STRINGS
import redis.protocol.NULL_BULK
import redis.protocol.OK
import redis.protocol.writeOutput
import java.io.OutputStream
import java.util.concurrent.ConcurrentHashMap

fun sET(
    output: OutputStream,
    parts: MutableList<String>,
    stringContainer: ConcurrentHashMap<String, STRINGS>
) {
    val setKey = parts.getOrNull(1) ?: ""
    val setValue = parts.getOrNull(2) ?: ""

    val secIdx = parts.indexOfFirst {
        it.equals("EX", ignoreCase = true) || it.equals("PX", ignoreCase = true)
    }

    val setExp = if (secIdx != -1 && secIdx + 1 < parts.size) {
        val ttl = parts[secIdx + 1].toLong()
        when (parts[secIdx].uppercase()) {
            "EX" -> System.currentTimeMillis() + ttl * 1000
            "PX" -> System.currentTimeMillis() + ttl
            else -> null
        }
    } else {
        null
    }

    stringContainer[setKey] = STRINGS(value = setValue, exp = setExp)
    writeOutput(output, OK)
}

fun gET(
    output: OutputStream,
    parts: MutableList<String>,
    stringContainer: ConcurrentHashMap<String, STRINGS>
) {
    val getKey = parts.getOrNull(1) ?: ""
    val entry = stringContainer[getKey]

    val response = if (entry == null) {
        NULL_BULK
    } else {
        val exp = entry.exp
        if (exp != null && System.currentTimeMillis() >= exp) {
            stringContainer.remove(getKey, entry)
            NULL_BULK
        } else {
            "\$${entry.value.length}\r\n${entry.value}\r\n"
        }
    }

    writeOutput(output, response)
}
