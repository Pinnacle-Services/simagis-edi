package net.paypredict.clients.api

import net.paypredict.clients.ppClients
import net.paypredict.clients.toStringPP
import java.util.*
import javax.json.Json
import javax.json.JsonArray
import javax.json.JsonObject
import javax.json.JsonValue
import javax.servlet.annotation.WebServlet
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 7/7/2018.
 */
@WebServlet(name = "ClientsApiServlet", urlPatterns = ["/api/*"], loadOnStartup = 1)
class ClientsApiServlet : HttpServlet() {

    override fun service(request: HttpServletRequest, response: HttpServletResponse) {
        try {
            super.service(request, response)
        } catch (e: Throwable) {
            val id = UUID.randomUUID().toString()
            System.err.print("$id: ")
            e.printStackTrace(System.err)
            response.sendJson(e.toErrorResponse(id), status = HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        }
    }

    override fun doGet(request: HttpServletRequest, response: HttpServletResponse) {
        request.checkUserRole("pp-api-admins")
        when (request.pathInfo) {
            "/all" -> response.sendJsonResult(getAllClients())
            else -> throw ClientsApiException("Invalid command: ${request.pathInfo}")
        }

    }

    private fun getAllClients(): JsonArray = Json
        .createArrayBuilder()
        .also { result ->
            ppClients().all.forEach { client ->
                result.add(
                    Json
                        .createObjectBuilder()
                        .add("id", client.id)
                        .build()
                )
            }
        }
        .build()

    private fun HttpServletRequest.checkUserRole(role: String) {
        if (!isUserInRole(role)) throw ClientsApiException("$role user role required")
    }

}

private fun HttpServletResponse.sendJsonResult(result: JsonValue) = sendJson(
    Json.createObjectBuilder()
        .add("result", result)
        .build()
)

private fun HttpServletResponse.sendJson(json: JsonObject, status: Int = HttpServletResponse.SC_OK) {
    this.status = status
    val bytes = json.toStringPP().toByteArray()
    contentType = "application/json"
    characterEncoding = "UTF-8"
    setContentLength(bytes.size)
    outputStream.write(bytes)
}

private fun Throwable.toErrorResponse(id: String): JsonObject =
    Json.createObjectBuilder()
        .add(
            "error", Json.createObjectBuilder()
                .add("class", this.javaClass.name)
                .add("message", this.message)
                .add("id", id)
        )
        .build()

private class ClientsApiException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)