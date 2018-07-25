package net.paypredict.digitalocean.update.api

import net.paypredict.digitalocean.update.*
import java.io.PrintWriter
import java.io.StringWriter
import java.util.concurrent.locks.ReentrantLock
import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonWriterFactory
import javax.json.stream.JsonGenerator
import javax.servlet.annotation.WebServlet
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import javax.servlet.http.HttpServletResponse.SC_OK
import kotlin.concurrent.thread
import kotlin.concurrent.withLock

/**
 * Created by alexei.vylegzhanin@gmail.com on 6/22/2018.
 */
@WebServlet(name = "UpdateApiServlet", urlPatterns = ["/api/*"], loadOnStartup = 1)
class UpdateApiServlet : HttpServlet() {
    private val autoUpdateFile = localClientDir.resolve("auto-update")

    override fun init() {
        daemon("watching auto-update") {
            while (true) {
                Thread.sleep(60_000)
                if (autoUpdateFile.isFile) {
                    try {
                        update()
                    } catch (e: UpdateApiException) {
                    }
                }
            }
        }
    }

    override fun doGet(request: HttpServletRequest, response: HttpServletResponse) {
        try {
            response.json = when (request.pathInfo) {
                "/local-ver" -> request.role("read") { localVer() }
                "/host-ver" -> request.role("read.host") { hostVer() }
                "/status" -> request.role("read") { status() }
                "/status-reset" -> request.role("admin") { resetStatus() }
                "/update" -> request.role("admin") { update() }
                "/install" -> request.role("admin") { install() }
                "/auto-update-start" -> request.role("admin") { autoUpdateStart() }
                "/auto-update-stop" -> request.role("admin") { autoUpdateStop() }
                else -> throw UpdateApiException("Invalid command: ${request.pathInfo}")
            }
        } catch (e: Throwable) {
            response.json = lock.withLock {
                status = Status.Error(e)
                status.toJson()
            }
        }
    }

    private fun HttpServletRequest.role(role: String, action: () -> JsonObject?): JsonObject? {
        if (!isUserInRole(role)) throw UpdateApiException("$role user role required")
        return action()
    }

    private val lock = ReentrantLock()
    private var status: Status = Status.Ready

    private fun hostVerData(): VerData =
        HostSFTP.new().session { readHostVer() }

    private fun hostVer(): JsonObject =
        HostSFTP.new().session { readHostVerJson() }

    private fun localVer(): JsonObject? =
        readLocalImageVerJson()

    private fun status(): JsonObject = lock.withLock { status.toJson() }

    private fun resetStatus(): JsonObject = lock.withLock {
        status = Status.Ready
        status.toJson()
    }

    private fun update(): JsonObject = lock.withLock {
        if (status !== Status.Ready) throw UpdateApiException("Invalid status: $status")
        status = Status.Loading
        daemon("update") {
            try {
                val localVer = readLocalImageVer()
                val hostVer = hostVerData()
                if (localVer != hostVer) {
                    val tmpImageDir = downloadPayPredict(hostVer)
                    lock.withLock { status = Status.Updating }
                    replaceLocalImage(tmpImageDir)
                    installLocalImage()
                }
                lock.withLock { status = Status.Ready }
            } catch (e: Throwable) {
                lock.withLock { status = Status.Error(e) }
                e.printStackTrace()
            }
        }
        status.toJson()
    }

    private fun install(): JsonObject = lock.withLock {
        if (status !== Status.Ready) throw UpdateApiException("Invalid status: $status")
        status = Status.Loading
        daemon("install") {
            try {
                lock.withLock { status = Status.Installing }
                installLocalImage()
                lock.withLock { status = Status.Ready }
            } catch (e: Throwable) {
                lock.withLock { status = Status.Error(e) }
                e.printStackTrace()
            }
        }
        status.toJson()
    }

    private fun autoUpdateStart(): JsonObject = lock.withLock {
        if (status !== Status.Ready) throw UpdateApiException("Invalid status: $status")
        autoUpdateFile.writeText("")
        status.toJson()
    }

    private fun autoUpdateStop(): JsonObject = lock.withLock {
        if (status !== Status.Ready) throw UpdateApiException("Invalid status: $status")
        autoUpdateFile.delete()
        status.toJson()
    }

    private fun daemon(name: String? = null, block: () -> Unit) =
        thread(block = block, isDaemon = true, name = name)
}

private class UpdateApiException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

private sealed class Status {
    open val text: String get() = javaClass.simpleName
    override fun toString(): String = text
    open fun toJson(): JsonObject =
        Json.createObjectBuilder()
            .add("status", javaClass.simpleName)
            .build()

    object Ready : Status()
    object Loading : Status()
    object Updating : Status()
    object Installing : Status()

    class Error(val x: Throwable) : Status() {
        override val text: String
            get() = StringWriter().also {
                PrintWriter(it).use { writer ->
                    writer.println("Error")
                    writer.print(x.javaClass.name + ": ")
                    x.printStackTrace(writer)
                }
            }.toString()

        override fun toString(): String = "Error: ${x.message}"

        override fun toJson(): JsonObject =
            Json.createObjectBuilder()
                .add("status", javaClass.simpleName)
                .add("error", x.javaClass.name)
                .add("message", x.message)
                .build()

    }
}

private var HttpServletResponse.json: JsonObject?
    get() = null
    set(value) {
        val bytes = value.toStringPP().toByteArray()
        status = SC_OK
        contentType = "text/plain;charset=UTF-8"
        setContentLength(bytes.size)
        outputStream.write(bytes)
    }

private val jsonPP: JsonWriterFactory = Json.createWriterFactory(
    mapOf(
        JsonGenerator.PRETTY_PRINTING to true
    )
)

private fun JsonObject?.toStringPP(): String = when {
    this == null -> "{}"
    else -> StringWriter().use { jsonPP.createWriter(it).write(this); it.toString().trim() }
}
