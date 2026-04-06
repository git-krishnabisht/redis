package redis

import redis.commands.*
import redis.models.LISTS
import redis.models.STREAM
import redis.models.STRINGS
import redis.protocol.writeOutput
import java.net.ServerSocket
import java.util.concurrent.ConcurrentHashMap

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
                        "PING"   -> pING(output)
                        "ECHO"   -> eCHO(output, parts)
                        "SET"    -> sET(output, parts, stringContainer)
                        "GET"    -> gET(output, parts, stringContainer)
                        "LPUSH"  -> lPUSH(output, parts, listContainer)
                        "RPUSH"  -> rPUSH(output, parts, listContainer)
                        "LRANGE" -> {
                            if (parts[2].toInt() >= 0 && parts[3].toInt() > 0) {
                                lRANGE_POS(output, parts, listContainer)
                            } else {
                                lRANGE_NEG(output, parts, listContainer)
                            }
                        }
                        "LLEN"   -> lLEN(output, parts, listContainer)
                        "LPOP"   -> when (parts.size) {
                            2    -> lPOP(output, parts, listContainer)
                            3    -> lPOP_RANGE(output, parts, listContainer)
                            else -> writeOutput(output, "-ERR wrong number of arguments for 'lpop' command\r\n")
                        }
                        "BLPOP"  -> bLPOP(output, parts, listContainer)
                        "TYPE"   -> tYPE(output, parts, listContainer, stringContainer, streamContainer)
                        "XADD"   -> xADD(output, parts, streamContainer)
                    }
                }
            }
        }.start()
    }
}
