package com.simagis.claims.rest.api

import com.simagis.claims.rest.api.jobs.Import
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import org.bson.Document
import java.net.HttpURLConnection
import java.net.HttpURLConnection.HTTP_BAD_REQUEST
import java.net.HttpURLConnection.HTTP_OK
import java.util.*
import javax.servlet.annotation.WebServlet
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/16/2017.
 */
@WebServlet(name = "ClaimApiServlet", urlPatterns = arrayOf("/api/*"))
class ClaimDbApiServlet : HttpServlet() {

    override fun doPost(request: HttpServletRequest, response: HttpServletResponse) {
        val action = request.pathInfo.split('/').let { path ->
            when (path.size) {
                2 -> path[1]
                else -> {
                    response.status = HttpURLConnection.HTTP_NOT_FOUND
                    return
                }
            }
        }

        when (action) {
            "jobs" -> Context(request, response).doGetJobList()
            "job" -> Context(request, response).doGetJob()
            "start" -> Context(request, response).doStartJob()
            "kill" -> Context(request, response).doKillJob()
            else -> response.status = HttpURLConnection.HTTP_NOT_FOUND
        }
    }

    override fun destroy() {
        ClaimDb.shutdown()
    }

    private fun Context.doGetJobList() = doJsonRequest {
        val status = opt("status")?.let { RJobStatus.valueOf(it) }
        val filter = status?.let { doc { `+`("status", status.name) } } ?: doc {}
        jsonOut.apply {
            `+`("jobs", ClaimDb.apiJobs
                    .find(filter)
                    .sort(doc { `+`("created", -1) })
                    .toList())
        }
    }

    private fun Context.doGetJob() = doJsonRequest {
        val id = get("id")
        val job = ClaimDb.apiJobs.find(doc(id)).first()
        jsonOut.apply { `+`("job", job) }
    }

    private fun Context.doStartJob() = doJsonRequest {
        val type = get("type")
        val options = jsonIn["options"] as? Document ?: doc {}
        when (type) {
            Import.TYPE -> jsonOut.apply { `+`("job", Import.start(options).toDoc()) }
            else -> throw ClaimDbApiException("Invalid job type: $type")
        }
    }

    private fun Context.doKillJob() = doJsonRequest {
        val id = get("id")
        RJobManager[id]
                ?.let {
                    val done = it.kill()
                    jsonOut.apply {
                        `+`("done", done)
                        `+`("job", it.toDoc())
                    }
                }
                ?: throw ClaimDbApiException("Job $id not found")
    }

    private class Context(
            private val request: HttpServletRequest,
            private val response: HttpServletResponse) {

        val jsonIn: Document by lazy {
            val estimatedSize = request.contentLength.let { if (it != -1) it else DEFAULT_BUFFER_SIZE }
            request.inputStream.readBytes(estimatedSize).let { body ->
                if (body.isEmpty())
                    Document() else
                    Document.parse(body.toString(request.characterEncoding?.let(::charset) ?: Charsets.UTF_8))
            }
        }
        val jsonOut = doc {}

        fun get(name: String): String = opt(name) ?:
                throw ClaimDbApiException("Invalid ${request.pathInfo} request: $name required")

        fun opt(name: String): String? = jsonIn[name] as? String

        fun doJsonRequest(body: Context.() -> Unit) {
            val requestContentType = request.contentType
            try {
                body()
                response.status = HTTP_OK
            } catch(e: Throwable) {
                response.status = HTTP_BAD_REQUEST
                val uuid = UUID.randomUUID().toString()
                jsonOut.appendError(e, uuid)
                request.servletContext.log("${e.javaClass.name}: ${e.message} ($uuid)", e)
            }

            val content = when(requestContentType) {
                "application/mongo-json" -> jsonOut.toStringPPM().toByteArray()
                else -> jsonOut.toStringPP().toByteArray()
            }

            response.contentType = "application/json"
            response.characterEncoding = "UTF-8"
            response.setContentLength(content.size)
            response.outputStream.write(content)
        }
    }
}
