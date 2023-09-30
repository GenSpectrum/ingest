package org.genspectrum.ingest.utils

import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.security.MessageDigest

@OptIn(ExperimentalStdlibApi::class)
fun hashMd5(obj: Any): String {
    val byteOut = ByteArrayOutputStream()
    val objectOut = ObjectOutputStream(byteOut)
    objectOut.writeObject(obj)
    objectOut.flush()
    val bytes = byteOut.toByteArray()
    val md = MessageDigest.getInstance("MD5")
    md.update(bytes)
    val digest = md.digest()
    return digest.toHexString()
}
