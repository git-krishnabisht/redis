import java.io.OutputStream
import java.net.ServerSocket
import java.util.concurrent.ConcurrentHashMap

data class EntrySTRINGS (
    val value: String,
    val exp: Long? = null
)

data class EntryLISTS (
    val exp: Long? = null
) {
    val rpushList by lazy {
        mutableListOf<String>()
    }
}

const val OK = "+OK\r\n"
const val NULL_BULK= "$-1\r\n"

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
    cacheStrings: ConcurrentHashMap<String, EntrySTRINGS>
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
    cacheStrings[setKey] = EntrySTRINGS(value = setValue, exp = setExp)
    writeOutput(output, OK)
}

fun gET (
    output: OutputStream,
    parts: MutableList<String>,
    cacheStrings: ConcurrentHashMap<String, EntrySTRINGS>
) {
    val getKey = parts.getOrNull(1) ?: ""
    val entry = cacheStrings[getKey]
    val response: String

    if (entry == null) {
        response = NULL_BULK
    } else {
        val now = System.currentTimeMillis()
        val exp = entry.exp

        if (exp != null && now >= exp) {
            cacheStrings.remove(getKey, entry)
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
    cacheList: ConcurrentHashMap<String, EntryLISTS>
) {
    val rpushKey = parts.getOrNull(1) ?: ""
    val rpushValue = parts.drop(2)

    val entry = cacheList.getOrPut(rpushKey) {
        EntryLISTS()
    }

    entry.rpushList.addAll(rpushValue)

    val listLen = entry.rpushList.size
    val response = ":$listLen\r\n"
    writeOutput(output, response)
}

fun lPUSH (
    output: OutputStream,
    parts: MutableList<String>,
    cacheList: ConcurrentHashMap<String, EntryLISTS>
) {
    val rpushKey = parts.getOrNull(1) ?: ""
    val rpushValue = parts.drop(2)

    val entry = cacheList.getOrPut(rpushKey) {
        EntryLISTS()
    }

    val values: MutableList<String> = mutableListOf()
    values.addAll(rpushValue)

    entry.rpushList.addAll(0, values.reversed())

    val listLen = entry.rpushList.size
    val response = ":$listLen\r\n"
    writeOutput(output, response)
}

fun lLEN (
    output: OutputStream,
    parts: MutableList<String>,
    cacheList: ConcurrentHashMap<String, EntryLISTS>
) {
    val listKey = parts.getOrNull(1) ?: ""
    val entry = cacheList.getOrPut(listKey) {
        EntryLISTS()
    }

    val listLen = entry.rpushList.size
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
    cacheList: ConcurrentHashMap<String, EntryLISTS>
) {
    val lrangeKey = parts.getOrNull(1) ?: ""
    val lrangeStartIdx = parts.getOrNull(2)!!.toInt()
    val lrangeStopIdx = parts.getOrNull(3)!!.toInt()
    val list = cacheList[lrangeKey]?.rpushList ?: mutableListOf()

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
    cacheList: ConcurrentHashMap<String, EntryLISTS>
) {
    val lrangeKey = parts.getOrNull(1) ?: ""
    var lrangeStartIdx = parts.getOrNull(2)!!.toInt()
    var lrangeStopIdx = parts.getOrNull(3)!!.toInt()

    val list = cacheList[lrangeKey]!!.rpushList
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
    cacheList: ConcurrentHashMap<String, EntryLISTS>
) {
    val lpopKey = parts.getOrNull(1) ?: ""

    val entry = cacheList.getOrPut(lpopKey) {
        EntryLISTS()
    }

    val response = if (entry.rpushList.isEmpty()) {
        "$${-1}\r\n"
    } else {
        "$${entry.rpushList[0].length}\r\n${entry.rpushList[0]}\r\n"
    }

    entry.rpushList.removeFirst()
    writeOutput(output, response)
}

fun lPOP_RANGE(
    output: OutputStream,
    parts: MutableList<String>,
    cacheList: ConcurrentHashMap<String, EntryLISTS>
) {
    val lpopKey = parts.getOrNull(1) ?: ""
    val lpopRange = parts.getOrNull(2)?.toIntOrNull()
    if (lpopRange == null || lpopRange <= 0) {
        writeOutput(output, "-ERR value is not an integer or out of range\r\n")
        return
    }

    val list = cacheList[lpopKey]?.rpushList
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

fun main(args: Array<String>) {
    val server = ServerSocket(6379)
    val cacheStrings = ConcurrentHashMap<String, EntrySTRINGS>()
    val cacheList = ConcurrentHashMap<String, EntryLISTS>()

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
                        "SET" -> sET(output, parts, cacheStrings)
                        "GET" -> gET(output, parts, cacheStrings)
                        "LPUSH" -> lPUSH(output, parts, cacheList)
                        "RPUSH" -> rPUSH(output, parts, cacheList)
                        "LRANGE" -> {
                            if (parts[2].toInt() >= 0 && parts[3].toInt() > 0) {
                                lRANGE_POS(output, parts, cacheList)
                            } else {
                                lRANGE_NEG(output, parts, cacheList)
                            }
                        }
                        "LLEN" -> lLEN(output, parts, cacheList)
                        "LPOP" -> {
                            when (parts.size) {
                                2 -> lPOP(output, parts, cacheList)
                                3 -> lPOP_RANGE(output, parts, cacheList)
                                else -> writeOutput(output, "-ERR wrong number of arguments for 'lpop' command\r\n")
                            }
                        }
                        "BLPOP" -> {
                            //TODO
                        }
                    }
                }
            }
        }.start()
    }
}