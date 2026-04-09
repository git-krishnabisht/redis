package redis.commands

import redis.models.STREAM
import redis.models.TTEM
import redis.protocol.writeOutput
import java.io.OutputStream
import java.util.concurrent.ConcurrentHashMap

fun validateStreamEntryIds(xaddEntryId: String, entry: STREAM, output: OutputStream): Boolean {
    val parts = xaddEntryId.split("-")
    val newLeft = parts[0].toInt()
    val newRightStar = parts[1]

    if (newLeft >= 0 && newRightStar == "*") {
        return true
    }

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
    val streamKey = parts.getOrNull(1)!!
    var entryId = parts.getOrNull(2)!!
    val fieldValues = parts.drop(3)
    val stream = streamContainer.getOrPut(streamKey) { STREAM() }
    val autoTimePart = System.currentTimeMillis().toString()
    val existingAutoSequences = TTEM[autoTimePart]
    val autoSequenceNum = if (existingAutoSequences.isNullOrEmpty()) {
        "0"
    } else {
        (existingAutoSequences.last().toInt() + 1).toString()
    }

    if (entryId == "*") {
        entryId = "${autoTimePart}-${autoSequenceNum}"
        TTEM.getOrPut(autoTimePart) { mutableListOf() }.add(autoSequenceNum)
    } else {
        val isEntryValid = validateStreamEntryIds(entryId, stream, output)

        if (!isEntryValid) return

        val idParts = entryId.split("-")
        val timePart = idParts[0]
        val rawSequence = idParts[1]
        if (rawSequence != "*") TTEM.getOrPut(timePart) { mutableListOf() }.add(rawSequence)

        var sequenceNum = 0

        if (rawSequence == "*") {
            if (timePart == "0" && TTEM[timePart].isNullOrEmpty()) {
                sequenceNum = 1
            } else {
                val existingSequences = TTEM[timePart]
                sequenceNum = if (existingSequences.isNullOrEmpty()) 0 else existingSequences.last().toInt() + 1
            }
            entryId = "${timePart}-${sequenceNum}"
            TTEM.getOrPut(timePart) { mutableListOf() }.add(sequenceNum.toString())
        }
    }

    val fields = stream.redisStream.getOrPut(entryId) { mutableMapOf() }

    var i = 0
    while (i + 1 < fieldValues.size) {
        fields[fieldValues[i]] = fieldValues[i + 1]
        i += 2
    }

    writeOutput(output, "\$${entryId.length}\r\n$entryId\r\n")
}
