import java.io.OutputStream
import java.net.ServerSocket
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock

data class STRINGS (
    val value: String,
    val exp: Long? = null
)

data class LISTS (
    val exp: Long? = null
) {
    val redisList: MutableList<String> = mutableListOf()
    val lock = ReentrantLock()
    val notEmpty: Condition = lock.newCondition()
}

data class STREAM (
    val exp: Long? = null
) {
    val redisStream: MutableMap<String,MutableMap<String, String>> = mutableMapOf()
}

const val OK = "+OK\r\n"
const val NULL_BULK = "$-1\r\n"
const val NULL_ARRAY = "*-1\r\n"

fun writeOutput (
    output: OutputStream,
    response: String
) {
    output.write(response.toByteArray())
    output.flush()
}

fun pING (
    output: OutputStream
) {
    writeOutput(output, "+PONG\r\n")
}

fun eCHO (
    output: OutputStream,
    parts: MutableList<String>
) {
    val echoMsg = parts.getOrNull(1) ?: ""
    val response = "$${echoMsg.length}\r\n$echoMsg\r\n"
    writeOutput(output, response)
}

fun sET (
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

fun gET (
    output: OutputStream,
    parts: MutableList<String>,
    stringContainer: ConcurrentHashMap<String, STRINGS>
) {
    val getKey = parts.getOrNull(1) ?: ""
    val entry = stringContainer[getKey]
    val response: String

    if (entry == null) {
        response = NULL_BULK
    } else {
        val now = System.currentTimeMillis()
        val exp = entry.exp

        if (exp != null && now >= exp) {
            stringContainer.remove(getKey, entry)
            response = NULL_BULK
        } else {
            response = "$${entry.value.length}\r\n${entry.value}\r\n"
        }
    }

    writeOutput(output, response)
}

fun rPUSH (
    output: OutputStream,
    parts: MutableList<String>,
    listContainer: ConcurrentHashMap<String, LISTS>
) {
    val rpushKey = parts.getOrNull(1) ?: ""
    val rpushValue = parts.drop(2)

    val entry = listContainer.getOrPut(rpushKey) {
        LISTS()
    }

    entry.lock.lock()
    var listLen:Int

    try {
        entry.redisList.addAll(rpushValue)
        entry.notEmpty.signalAll()
        listLen = entry.redisList.size
    } finally {
        entry.lock.unlock()
    }

    val response = ":$listLen\r\n"
    writeOutput(output, response)
}

fun popLeft(
    entry: LISTS,
    blocking: Boolean,
    timeoutMs: Long? = null
): String? {
    entry.lock.lock()
    try{
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

        return  entry.redisList.removeAt(0)
    } finally {
        entry.lock.unlock()
    }
}

fun lPUSH (
    output: OutputStream,
    parts: MutableList<String>,
    listContainer: ConcurrentHashMap<String, LISTS>
) {
    val rpushKey = parts.getOrNull(1) ?: ""
    val rpushValue = parts.drop(2)

    val entry = listContainer.getOrPut(rpushKey) {
        LISTS()
    }

    val values: MutableList<String> = mutableListOf()
    values.addAll(rpushValue)

    entry.lock.lock()
    var listLen: Int

    try {
        entry.redisList.addAll(0, values.reversed())
        entry.notEmpty.signalAll()
        listLen = entry.redisList.size
    } finally {
        entry.lock.unlock()
    }

    val response = ":$listLen\r\n"
    writeOutput(output, response)
}

fun lLEN (
    output: OutputStream,
    parts: MutableList<String>,
    listContainer: ConcurrentHashMap<String, LISTS>
) {
    val listKey = parts.getOrNull(1) ?: ""
    val entry = listContainer.getOrPut(listKey) {
        LISTS()
    }

    val listLen = entry.redisList.size
    val response = if (listLen > 0) {
        ":${listLen}\r\n"
    } else {
        ":0\r\n"
    }
    writeOutput(output, response)
}

fun lRANGE_POS(
    output: OutputStream,
    parts: MutableList<String>,
    listContainer: ConcurrentHashMap<String, LISTS>
) {
    val lrangeKey = parts.getOrNull(1) ?: ""
    val lrangeStartIdx = parts.getOrNull(2)!!.toInt()
    val lrangeStopIdx = parts.getOrNull(3)!!.toInt()
    val list = listContainer[lrangeKey]?.redisList ?: mutableListOf()

    if (list.isEmpty() || lrangeStartIdx > lrangeStopIdx || lrangeStartIdx >= list.size) {
        writeOutput(output, "*0\r\n")
        return
    }

    val safeStart = lrangeStartIdx.coerceAtLeast(0)
    val safeStop = lrangeStopIdx.coerceAtMost(list.size - 1)

    val lrangeList = list.subList(safeStart, safeStop + 1)

    var response = "*${lrangeList.size}\r\n"
    lrangeList.forEach { value ->
        response += "$${value.length}\r\n$value\r\n"
    }

    writeOutput(output, response)
}

fun lRANGE_NEG(
    output: OutputStream,
    parts: MutableList<String>,
    listContainer: ConcurrentHashMap<String, LISTS>
) {
    val lrangeKey = parts.getOrNull(1) ?: ""
    var lrangeStartIdx = parts.getOrNull(2)!!.toInt()
    var lrangeStopIdx = parts.getOrNull(3)!!.toInt()

    val list = listContainer[lrangeKey]!!.redisList
    val listLen = list.size

    lrangeStartIdx = if (lrangeStartIdx < 0) listLen + lrangeStartIdx else lrangeStartIdx
    lrangeStopIdx = if (lrangeStopIdx < 0) listLen + lrangeStopIdx else lrangeStopIdx

    if (list.isEmpty() || lrangeStartIdx > lrangeStopIdx || lrangeStartIdx >= list.size) {
        writeOutput(output, "*0\r\n")
        return
    }

    val safeStart = lrangeStartIdx.coerceAtLeast(0)
    val safeStop = lrangeStopIdx.coerceAtMost(list.size - 1)

    val lrangeList = list.subList(safeStart, safeStop + 1)

    var response = "*${lrangeList.size}\r\n"
    lrangeList.forEach { value ->
        response += "$${value.length}\r\n$value\r\n"
    }

    writeOutput(output, response)
}

fun lPOP (
    output: OutputStream,
    parts: MutableList<String>,
    listContainer: ConcurrentHashMap<String, LISTS>
) {
    val lpopKey = parts.getOrNull(1) ?: ""

    val entry = listContainer.getOrPut(lpopKey) {
        LISTS()
    }

    val value = popLeft(entry, blocking = false)

    val response = if (value == null) {
        "$${-1}\r\n"
    } else {
        "$${value.length}\r\n$value\r\n"
    }

    writeOutput(output, response)
}

fun lPOP_RANGE(
    output: OutputStream,
    parts: MutableList<String>,
    listContainer: ConcurrentHashMap<String, LISTS>
) {
    val lpopKey = parts.getOrNull(1) ?: ""
    val lpopRange = parts.getOrNull(2)?.toIntOrNull()
    if (lpopRange == null || lpopRange <= 0) {
        writeOutput(output, "-ERR value is not an integer or out of range\r\n")
        return
    }

    val list = listContainer[lpopKey]?.redisList
    if (list.isNullOrEmpty()) {
        writeOutput(output, "*0\r\n")
        return
    }

    var response: String = ""

    if (list.size < lpopRange) {
        response = buildString {
            append("*${list.size}\r\n")
            list.forEach { value ->
                append("$${value.length}\r\n")
                append(value)
                append("\r\n")
            }
        }

        list.clear()
    } else  {
        response = "*$lpopRange\r\n"
        for (i in 0 until lpopRange) {
            response += "$${list[i].length}\r\n${list[i]}\r\n"
        }
        list.subList(0, lpopRange).clear()
    }

    writeOutput(output, response)
}

fun bLPOP(
    output: OutputStream,
    parts: MutableList<String>,
    listContainer: ConcurrentHashMap<String, LISTS>
) {
    val lpopKey = parts.getOrNull(1) ?: ""
    val timeoutSeconds = parts.getOrNull(2)?.toDoubleOrNull() ?: 0.0
    val timeoutMs = (timeoutSeconds * 1_000).toLong()

    val entry = listContainer.getOrPut(lpopKey) {
        LISTS()
    }

    val value = popLeft(entry, blocking = true, timeoutMs = timeoutMs)

    val response = if (value == null) {
        NULL_ARRAY
    } else {
        "*2\r\n$${lpopKey.length}\r\n$lpopKey\r\n$${value.length}\r\n$value\r\n"
    }

    writeOutput(output, response)
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

fun validateStreamEntryIds(
    xaddEntryId: String,
    entry: STREAM,
    output: OutputStream
): Boolean {
    val newLeft = xaddEntryId.split("-")[0].toInt()
    val newRight = xaddEntryId.split("-")[1].toInt()

    if (newLeft == 0 && newRight == 0) {
        writeOutput(output, "-ERR The ID specified in XADD must be greater than 0-0\r\n")
        return false
    }

    val streamIsEmpty = entry.redisStream.isEmpty()
    if (streamIsEmpty) return true

    val lastEntryId = entry.redisStream.keys.last()
    val lastLeft = lastEntryId.split("-")[0].toInt()
    val lastRight = lastEntryId.split("-")[1].toInt()

    if (lastLeft > newLeft) {
        writeOutput(output, "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
        return false
    } else if (lastLeft == newLeft) {
        if (newRight > lastRight) {
            return true
        }
        writeOutput(output, "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
        return false
    }
    return true
}


fun xADD(
    output: OutputStream,
    parts: MutableList<String>,
    streamContainer: ConcurrentHashMap<String, STREAM>
) {
    val xaddKey = parts.getOrNull(1)!!
    val xaddEntryId = parts.getOrNull(2)!!
    val keyValues = parts.drop(3)
    val listLen = keyValues.size

    val entry = streamContainer.getOrPut(xaddKey) {
        STREAM()
    }

    val valid = validateStreamEntryIds(xaddEntryId, entry, output)

    if (!valid) return

    val entryMap = entry.redisStream.getOrPut(xaddEntryId) { mutableMapOf() }

    var response = ""

    var i = 0
    while (i + 1 < listLen) {
        entryMap[keyValues[i]] = keyValues[i + 1]
        i += 2
    }

    response = "$${xaddEntryId.length}\r\n${xaddEntryId}\r\n"
    writeOutput(output, response)
}



fun main(args: Array<String>) {
    val server = ServerSocket(6379)
    val stringContainer = ConcurrentHashMap<String, STRINGS>()
    val listContainer = ConcurrentHashMap<String, LISTS>()
    val streamContainer = ConcurrentHashMap<String, STREAM>()

    while (true) {
        val conn = server.accept()

        Thread {
            conn.use { socket ->
                val reader = socket.getInputStream().bufferedReader()
                val output = socket.getOutputStream()

                while (true) {
                    val line = reader.readLine() ?: break
                    if (!line.startsWith("*")) continue

                    val argCount = line.substring(1).toInt()
                    val parts = mutableListOf<String>()

                    repeat(argCount) {
                        reader.readLine()
                        val value = reader.readLine() ?: ""
                        parts.add(value)
                    }

                    when (parts.getOrNull(0)?.uppercase()) {
                        "PING" -> pING(output)
                        "ECHO" -> eCHO(output, parts)
                        "SET" -> sET(output, parts, stringContainer)
                        "GET" -> gET(output, parts, stringContainer)
                        "LPUSH" -> lPUSH(output, parts, listContainer)
                        "RPUSH" -> rPUSH(output, parts, listContainer)
                        "LRANGE" -> {
                            if (parts[2].toInt() >= 0 && parts[3].toInt() > 0){
                                lRANGE_POS(output, parts, listContainer)
                            } else {
                                lRANGE_NEG(output, parts, listContainer)
                            }
                        }
                        "LLEN" -> lLEN(output, parts, listContainer)
                        "LPOP" -> {
                            when (parts.size) {
                                2 -> lPOP(output, parts, listContainer)
                                3 -> lPOP_RANGE(output, parts, listContainer)
                                else -> writeOutput(output, "-ERR wrong number of arguments for 'lpop' command\r\n")
                            }
                        }
                        "BLPOP" -> bLPOP(output, parts, listContainer)
                        "TYPE" -> tYPE(output, parts, listContainer, stringContainer, streamContainer)
                        "XADD" -> xADD(output, parts, streamContainer)
                    }
                }
            }
        }.start()
    }
}

