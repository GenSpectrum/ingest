package org.genspectrum.ingest.utils

import com.alibaba.fastjson2.JSON
import com.alibaba.fastjson2.JSONObject
import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader

class NdjsonReader(inputStream: InputStream) : Iterator<JSONObject>, Iterable<JSONObject>, AutoCloseable {

    private val reader: BufferedReader
    private var nextEntry: JSONObject? = null

    init {
        reader = BufferedReader(InputStreamReader(inputStream))
        read()
    }

    override fun hasNext(): Boolean {
        return nextEntry != null
    }

    override fun next(): JSONObject {
        val entry = nextEntry ?: throw NoSuchElementException("No element available")
        read()
        return entry
    }

    private fun read() {
        val line = reader.readLine()
        nextEntry = if (line == null) {
            null
        } else {
            JSON.parseObject(line)
        }
    }

    override fun close() {
        reader.close()
    }

    override fun iterator(): Iterator<JSONObject> {
        return this
    }

}
