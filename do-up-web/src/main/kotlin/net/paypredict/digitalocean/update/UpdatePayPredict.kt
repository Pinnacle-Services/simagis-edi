package net.paypredict.digitalocean.update

import com.jcraft.jsch.ChannelSftp
import java.io.File
import java.util.*
import javax.json.JsonObject

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 6/17/2018.
 */

fun main(args: Array<String>) {
    downloadPayPredict()
}

data class VerData(val id: String = "", val time: String = "", val exists: Boolean = false)

fun readLocalImageVer(): VerData =
    localClientImageDir
        .resolve("ver.json")
        .let { file ->
            if (file.exists())
                file.reader().use { it.readJson() }.toVerData()
            else
                VerData(exists = false)
        }

fun ChannelSftp.readHostVer(): VerData {
    cd("/clients/$tagsClient/upload")
    return get("ver.json").reader().use { it.readJson() }.toVerData()
}

private fun JsonObject.toVerData(): VerData {
    val time = getString("time")
    val id = getString("id", time)
    return VerData(id, time, exists = true)
}

fun downloadPayPredict(ver: VerData? = null): File = HostSFTP.new().session {

    fun downloadDir(sftpPath: String, dstDir: File) {
        val entryList = ls(sftpPath).toList().filterIsInstance<ChannelSftp.LsEntry>()
        println(sftpPath)
        cd(sftpPath)
        dstDir.mkdir()
        entryList
            .filter { it.attrs.isDir && !it.attrs.isLink && !it.filename.startsWith(".") }
            .forEach {
                downloadDir("$sftpPath/${it.filename}", dstDir.resolve(it.filename))
            }
        cd(sftpPath)
        entryList
            .filter { !it.attrs.isDir && !it.attrs.isLink }
            .forEach { entry ->
                print(entry.longname)
                dstDir.resolve(entry.filename).outputStream().use { file ->
                    get(entry.filename).use { sftp -> sftp.copyTo(file) }
                }
                println()
            }

    }

    val tempDir = localClientTmpDir
        .resolve("uploads-" + UUID.randomUUID().toString()).also { it.mkdir() }

    val version1 = ver ?: readHostVer()
    downloadDir("/clients/$tagsClient/upload", tempDir)
    val version2 = readHostVer()
    if (version1 != version2) throw DigitalOceanUpdateException(
        "The /upload/version.json has been changed during download: $version1 != $version2"
    )
    tempDir
}

fun replaceLocalImage(tmpImageDir: File) {
    val oldVerDir = localClientTmpDir.resolve("old-ver-" + UUID.randomUUID().toString())
    localClientImageDir.renameTo(oldVerDir)
    tmpImageDir.renameTo(localClientImageDir)
}
