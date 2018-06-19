package net.paypredict.digitalocean.update

import com.jcraft.jsch.ChannelSftp
import java.io.File
import java.util.*

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 6/17/2018.
 */

fun main(args: Array<String>) {
    updatePayPredict()
}

data class UploadVersion(val id: String, val time: String)

private fun ChannelSftp.readUploadVersion(): UploadVersion {
    cd("/clients/$tagsClient/upload")
    return get("version.json").reader().use { it.readJson() }.let { json ->
        UploadVersion(
            id = json.getString("id"),
            time = json.getString("time")
        )
    }
}

fun updatePayPredict() {
    HostSFTP.new().session {

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

        val tempDir = tagsClientDir
            .resolve(".tmp").also { it.mkdir() }
            .resolve("uploads-" + UUID.randomUUID().toString()).also { it.mkdir() }

        val version1 = readUploadVersion()
        downloadDir("/clients/$tagsClient/upload", tempDir)
        val version2 = readUploadVersion()
        if (version1 != version2) throw DigitalOceanUpdateException(
            "The /upload/version.json has been changed during download: $version1 != $version2"
        )
    }
}

