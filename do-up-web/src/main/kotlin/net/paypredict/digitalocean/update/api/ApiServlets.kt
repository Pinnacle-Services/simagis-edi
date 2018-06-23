package net.paypredict.digitalocean.update.api

import net.paypredict.digitalocean.update.*
import java.io.PrintWriter
import java.io.StringWriter
import java.util.concurrent.locks.ReentrantLock
import javax.servlet.annotation.WebServlet
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import javax.servlet.http.HttpServletResponse.SC_OK
import kotlin.concurrent.thread
import kotlin.concurrent.withLock

/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 6/22/2018.
 */
@WebServlet(name = "ApiServlet", urlPatterns = ["/api/*"])
class ApiServlet : HttpServlet() {

    override fun doGet(request: HttpServletRequest, response: HttpServletResponse) {
        try {
            when (request.pathInfo) {
                "/host-ver" -> response.text = hostVer().toString()
                "/status" -> response.text = status()
                "/update" -> response.text = update()
                "/auto-update-start" -> response.text = autoUpdateStart()
                "/auto-update-stop" -> response.text = autoUpdateStop()
                "/reset" -> response.text = reset()
                else -> throw ApiException("Invalid command: ${request.pathInfo}")
            }
        } catch (e: Throwable) {
            lock.withLock { status = Status.Error(e) }
            response.text = status()
        }
    }

    private val lock = ReentrantLock()
    private var status: Status = Status.Ready

    private fun hostVer(): VerData =
        HostSFTP.new().session { readHostVer() }

    private fun status(): String = lock.withLock { status.text }

    private fun reset(): String = lock.withLock {
        status = Status.Ready
        status.text
    }

    private fun update(): String = lock.withLock {
        if (status !== Status.Ready) throw ApiException("Invalid status: $status")
        status = Status.Loading
        thread {
            try {
                val localVer = readLocalImageVer()
                val hostVer = hostVer()
                if (localVer != hostVer) {
                    val tmpImageDir = downloadPayPredict(hostVer)
                    lock.withLock { status = Status.Updating }
                    replaceLocalImage(tmpImageDir)
                    lock.withLock { status = Status.Ready }
                }
            } catch (e: Throwable) {
                lock.withLock { status = Status.Error(e) }
            }
        }
        "Update started"
    }

    private fun autoUpdateStart(): String = lock.withLock {
        if (status !== Status.Ready) throw ApiException("Invalid status: $status")
        status = Status.AutoUpdate
        "Auto-Update started"
    }

    private fun autoUpdateStop(): String = lock.withLock {
        if (status !== Status.AutoUpdate) throw ApiException("Invalid status: $status")
        status = Status.Ready
        "Auto-Update stopped"
    }
}

private class ApiException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

private sealed class Status {
    open val text: String get() = javaClass.simpleName
    override fun toString(): String = text

    object Ready : Status()
    object Loading : Status()
    object Updating : Status()
    object AutoUpdate : Status()

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
    }
}

private var HttpServletResponse.text: String
    get() = ""
    set(value) {
        val bytes = value.toByteArray()
        status = SC_OK
        contentType = "text/plain;charset=UTF-8"
        setContentLength(bytes.size)
        outputStream.write(bytes)
    }