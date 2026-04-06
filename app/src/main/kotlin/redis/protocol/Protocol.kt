package redis.protocol

import java.io.OutputStream

const val OK = "+OK\r\n"
const val NULL_BULK = "\$-1\r\n"
const val NULL_ARRAY = "*-1\r\n"

fun writeOutput(output: OutputStream, response: String) {
    output.write(response.toByteArray())
    output.flush()
}
