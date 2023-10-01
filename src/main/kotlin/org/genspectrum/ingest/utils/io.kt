package org.genspectrum.ingest.utils

import com.alibaba.fastjson2.JSONWriter
import com.alibaba.fastjson2.parseObject
import com.alibaba.fastjson2.toJSONByteArray
import com.github.luben.zstd.ZstdInputStream
import com.github.luben.zstd.ZstdOutputStream
import org.tukaani.xz.LZMA2Options
import org.tukaani.xz.XZInputStream
import org.tukaani.xz.XZOutputStream
import java.io.*
import java.nio.file.Path
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.io.path.extension

fun readFile(file: Path): InputStream {
    var inputStream: InputStream = FileInputStream(file.toFile())
    if (file.extension == "zst") {
        inputStream = ZstdInputStream(inputStream)
    } else if (file.extension == "xz") {
        inputStream = XZInputStream(inputStream)
    }
    return inputStream
}

fun writeFile(file: Path): OutputStream {
    var outputStream: OutputStream = BufferedOutputStream(FileOutputStream(file.toFile()))
    if (file.extension == "zst") {
        outputStream = ZstdOutputStream(outputStream)
    } else if (file.extension == "xz") {
        outputStream = XZOutputStream(outputStream, LZMA2Options())
    }
    return BufferedOutputStream(outputStream)
}

inline fun <reified T> readNdjson(inputStream: InputStream, bufferQueueSize:Int = 100): Sequence<T> {
    return sequence {
        val lineQueue = LinkedBlockingQueue<String>(bufferQueueSize)
        val entryQueue = LinkedBlockingQueue<T>(bufferQueueSize)

        val fetchingThread = thread(start = true, name = "readNdjson.fetchingThread") {
            BufferedReader(InputStreamReader(inputStream), 524288).use { reader ->
                while (true) {
                    val nextEntry = reader.readLine() ?: break
                    lineQueue.put(nextEntry)
                }
            }
        }

        val parsingThread = thread(start = true, name = "readNdjson.parsingThread") {
            while (lineQueue.isNotEmpty() || fetchingThread.isAlive) {
                val nextLine = lineQueue.poll(1, TimeUnit.SECONDS) ?: continue
                val nextEntry = nextLine.parseObject<T>()
                entryQueue.put(nextEntry)
            }
        }

        while (entryQueue.isNotEmpty() || parsingThread.isAlive) {
            yield(entryQueue.poll(1, TimeUnit.SECONDS) ?: continue)
        }
    }
}

fun <T> writeNdjson(
    outputStream: OutputStream,
    returnHash: ((entry: T, hash: String) -> Unit)? = null,
    bufferQueueSize: Int = 100
): WriteNdjsonResponse<T> {
    val queue = LinkedBlockingQueue<T>(bufferQueueSize)
    var closed = false

    val writingThread = thread(start = true, name = "writeNdjson.writingThread") {
        outputStream.use { outputStream ->
            while (queue.isNotEmpty() || !closed) {
                val entry = queue.poll(1, TimeUnit.SECONDS) ?: continue
                val entrySerialized = entry.toJSONByteArray(
                    JSONWriter.Feature.WriteNulls,
                    JSONWriter.Feature.MapSortField
                )
                outputStream.write(entrySerialized)
                outputStream.write("\n".toByteArray())
                if (returnHash != null) {
                    val hash = hashMd5(entrySerialized)
                    returnHash(entry, hash)
                }
            }
        }
    }

    return WriteNdjsonResponse(
        fun (entry: T) {
            queue.put(entry)
        },
        fun () {
            closed = true
            writingThread.join()
        }
    )
}

data class WriteNdjsonResponse<T> (
    val write: (T) -> Unit,
    val close: () -> Unit
)
