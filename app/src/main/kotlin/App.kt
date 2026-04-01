import java.net.ServerSocket
import java.util.concurrent.ConcurrentHashMap


fun main(args: Array<String>) {
    val server = ServerSocket(6379)
    val cache = ConcurrentHashMap<String, String>()
    val oK = "+OK\r\n"
    val nullBulk = "$-1\r\n"

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

                    when (parts.getOrNull(0)) {
                        "PING" -> {
                            output.write("+PONG\r\n".toByteArray())
                            output.flush()
                        }

                        "ECHO" -> {
                            val echoMsg = parts.getOrNull(1) ?: ""
                            val response = "$${echoMsg.length}\r\n$echoMsg\r\n"
                            output.write(response.toByteArray())
                            output.flush()
                        }

                        "SET" -> {
                            val setKey = parts.getOrNull(1) ?: ""
                            val setValue = parts.drop(2).takeWhile { it != "EX" && it != "PX" }.joinToString(" ")
                            val secIdx = parts.indexOfFirst { it == "EX" || it == "PX" }

                            cache[setKey] = setValue

                            if (secIdx != -1) {
                                val secCommand = parts[secIdx]
                                val time = parts[secIdx + 1].toLong()
                                val millis = if (secCommand == "EX") time * 1000 else time

                                Thread {
                                    Thread.sleep(millis)
                                    cache.remove(setKey)
                                }.start()
                            }


                            output.write(oK.toByteArray())
                            output.flush()
                        }

                        "GET" -> {
                            val getKey = parts.getOrNull(1)
                            val getValue = cache[getKey]

                            if (getValue == null) {
                                output.write(nullBulk.toByteArray())
                            } else {
                                output.write($$"$$${getValue.length}\r\n$$getValue\r\n".toByteArray())
                            }
                            output.flush()
                        }
                    }
                }
            }
        }.start()
    }
}