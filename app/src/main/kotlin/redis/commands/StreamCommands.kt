package redis.commands

import redis.models.STREAM
import redis.protocol.writeOutput
import java.io.OutputStream
import java.util.concurrent.ConcurrentHashMap

fun validateStreamEntryIds(xaddEntryId: String, entry: STREAM, output: OutputStream): Boolean {
    val parts = xaddEntryId.split("-")
    val newLeft = parts[0].toInt()
    val newRight = parts[1].toInt()

    if (newLeft == 0 && newRight == 0) {
        writeOutput(output, "-ERR The ID specified in XADD must be greater than 0-0\r\n")
        return false
    }

    if (entry.redisStream.isEmpty()) return true

    val lastId = entry.redisStream.keys.last().split("-")
    val lastLeft = lastId[0].toInt()
    val lastRight = lastId[1].toInt()

    return when {
        lastLeft > newLeft -> {
            writeOutput(output, "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
            false
        }
        lastLeft == newLeft && newRight <= lastRight -> {
            writeOutput(output, "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
            false
        }
        else -> true
    }
}

fun xADD(
    output: OutputStream,
    parts: MutableList<String>,
    streamContainer: ConcurrentHashMap<String, STREAM>
) {
    val key = parts.getOrNull(1)!!
    val entryId = parts.getOrNull(2)!!
    val keyValues = parts.drop(3)

    val entry = streamContainer.getOrPut(key) { STREAM() }

    if (!validateStreamEntryIds(entryId, entry, output)) return

    val entryMap = entry.redisStream.getOrPut(entryId) { mutableMapOf() }
    var i = 0
    while (i + 1 < keyValues.size) {
        entryMap[keyValues[i]] = keyValues[i + 1]
        i += 2
    }

    writeOutput(output, "\$${entryId.length}\r\n$entryId\r\n")
}
