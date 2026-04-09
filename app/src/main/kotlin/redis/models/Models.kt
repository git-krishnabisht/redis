package redis.models

import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock

data class STRINGS(
    val value: String,
    val exp: Long? = null
)

data class LISTS(
    val exp: Long? = null
) {
    val redisList: MutableList<String> = mutableListOf()
    val lock = ReentrantLock()
    val notEmpty: Condition = lock.newCondition()
}

data class STREAM(
    val exp: Long? = null
) {
    val redisStream: MutableMap<String, MutableMap<String, String>> = mutableMapOf()
}

val TTEM: MutableMap<String, MutableList<String>> = mutableMapOf()

