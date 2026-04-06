package redis.commands

import redis.models.LISTS
import redis.models.STREAM
import redis.models.STRINGS
import redis.protocol.writeOutput
import java.io.OutputStream
import java.util.concurrent.ConcurrentHashMap

fun pING(output: OutputStream) {
    writeOutput(output, "+PONG\r\n")
}

fun eCHO(output: OutputStream, parts: MutableList<String>) {
    val echoMsg = parts.getOrNull(1) ?: ""
    writeOutput(output, "\$${echoMsg.length}\r\n$echoMsg\r\n")
}

fun tYPE(
    output: OutputStream,
    parts: MutableList<String>,
    listContainer: ConcurrentHashMap<String, LISTS>,
    stringContainer: ConcurrentHashMap<String, STRINGS>,
    streamContainer: ConcurrentHashMap<String, STREAM>
) {
    val typeKey = parts.getOrNull(1) ?: ""

    val stringEntry = stringContainer[typeKey]
    val listEntry = listContainer[typeKey]
    val streamEntry = streamContainer[typeKey]

    val response = when {
        stringEntry != null -> {
            val exp = stringEntry.exp
            if (exp != null && System.currentTimeMillis() >= exp) {
                stringContainer.remove(typeKey, stringEntry)
                "+none\r\n"
            } else {
                "+string\r\n"
            }
        }
        listEntry != null && listEntry.redisList.isNotEmpty() -> "+list\r\n"
        streamEntry != null && streamEntry.redisStream.isNotEmpty() -> "+stream\r\n"
        else -> "+none\r\n"
    }

    writeOutput(output, response)
}
