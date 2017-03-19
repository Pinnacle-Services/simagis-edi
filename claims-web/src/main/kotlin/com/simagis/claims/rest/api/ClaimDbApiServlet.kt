package com.simagis.claims.rest.api

import com.simagis.claims.rest.api.jobs.Import
import java.net.HttpURLConnection
import java.net.HttpURLConnection.HTTP_BAD_REQUEST
import java.net.HttpURLConnection.HTTP_OK
import java.util.*
import javax.json.Json
import javax.json.JsonObject
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

    override fun doGet(request: HttpServletRequest, response: HttpServletResponse) {
        val path = request.pathInfo.split('/')
        if (path.size < 2) {
            response.status = HttpURLConnection.HTTP_NOT_FOUND
            return
        }
        val action = path[1]

        when (action) {
            "jobs" -> JsonRequest(request, response, path).getJobList()
            "job" -> JsonRequest(request, response, path).getJob()
            "kill" -> JsonRequest(request, response, path).killJob()
            "start" -> JsonRequest(request, response, path).startJob()
            else -> response.status = HttpURLConnection.HTTP_NOT_FOUND
        }
    }

    override fun destroy() {
        ClaimDb.shutdown()
    }

    private fun JsonRequest.getJobList() = doJsonRequest {
        val status = path.getOrNull(2)?.let { JobStatus.valueOf(it) }
        jsonOut = Json
                .createObjectBuilder()
                .add("jobs", Json.createArrayBuilder().apply {
                    JobManager.find(status).forEach {
                        add(Job.of(it).toJson())
                    }
                })
                .build()
    }

    private fun JsonRequest.getJob() = doJsonRequest {
        val id = path.getOrElse(2) {
            throw ClaimDbApiException("Invalid job request")
        }
        jsonOut = JobManager[id]?.toJson()?.build()
                ?: throw ClaimDbApiException("Job $id not found")
    }

    private fun JsonRequest.startJob() = doJsonRequest {
        val type = path.getOrElse(2) {
            throw ClaimDbApiException("Invalid start request")
        }
        jsonOut = when (type) {
            Import.TYPE -> Import.start().toJson().build()
            else -> throw ClaimDbApiException("Invalid job type: $type")
        }
    }

    private fun JsonRequest.killJob() = doJsonRequest {
        val id = path.getOrElse(2) {
            throw ClaimDbApiException("Invalid kill request")
        }
        jsonOut = JobManager[id]
                ?.let {
                    val done = it.kill()
                    Json.createObjectBuilder().add("done", done).build()
                }
                ?: throw ClaimDbApiException("Job $id not found")
    }

    private class JsonRequest(
            val request: HttpServletRequest,
            val response: HttpServletResponse,
            val path: List<String>) {

        val jsonIn: JsonObject by lazy { request.inputStream.use { Json.createReader(it).readObject() } }
        var jsonOut: JsonObject? = null

        fun doJsonRequest(body: JsonRequest.() -> Unit) {
            try {
                this.body()
                response.status = HTTP_OK
            } catch(e: Throwable) {
                response.status = HTTP_BAD_REQUEST
                val uuid = UUID.randomUUID().toString()
                jsonOut = e.toErrorJsonObject(uuid)
                request.servletContext.log("${e.javaClass.name}: ${e.message} ($uuid)", e)
            }

            val content = jsonOut.toByteArray()
            response.contentType = "application/json"
            response.characterEncoding = "UTF-8"
            response.setContentLength(content.size)
            response.outputStream.write(content)
        }
    }
}
