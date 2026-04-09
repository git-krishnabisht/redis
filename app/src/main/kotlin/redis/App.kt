package redis

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import redis.commands.*
import redis.models.LISTS
import redis.models.STREAM
import redis.models.STRINGS
import redis.protocol.writeOutput
import java.net.InetAddress
import java.net.ServerSocket
import java.util.concurrent.ConcurrentHashMap

class RedisServer : CliktCommand(
    name = "redis-kt",
    help = "A lightweight Redis server implemented in Kotlin."
) {
    private val port by option("-p", "--port", help = "Port to listen on")
        .int()
        .default(6379)

    private val bind by option("--bind", help = "Address to bind to")
        .default("0.0.0.0")

    override fun run() {
        val stringContainer = ConcurrentHashMap<String, STRINGS>()
        val listContainer = ConcurrentHashMap<String, LISTS>()
        val streamContainer = ConcurrentHashMap<String, STREAM>()

        val addr = InetAddress.getByName(bind)
        val server = ServerSocket(port, 50, addr)

        Runtime.getRuntime().addShutdownHook(Thread {
            echo("Shutting down...")
            server.close()
        })

        echo("redis-kt server started")
        echo("  PID:  ${ProcessHandle.current().pid()}")
        echo("  Bind: $bind")
        echo("  Port: $port")
        echo("Ready to accept connections.")

        while (!server.isClosed) {
            val conn = try {
                server.accept()
            } catch (_: Exception) {
                break
            }

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
}

fun main(args: Array<String>) = RedisServer().main(args.toList())
