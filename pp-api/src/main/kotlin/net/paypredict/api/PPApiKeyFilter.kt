package net.paypredict.api

import java.io.File
import java.security.Principal
import java.util.concurrent.locks.ReentrantLock
import javax.json.Json
import javax.json.JsonArray
import javax.json.JsonObject
import javax.json.JsonString
import javax.servlet.*
import javax.servlet.annotation.WebFilter
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletRequestWrapper
import javax.servlet.http.HttpServletResponse
import kotlin.concurrent.withLock

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 7/5/2018.
 */
@WebFilter(filterName = "PP-API-KEY-FILTER", urlPatterns = ["/api/*"])
class PPApiKeyFilter : Filter {
    private val lock = ReentrantLock()
    private val keys = mutableMapOf<String, ApiKey>()
    private val keysDir = File("/PayPredict/api-keys")

    private fun get(key: String): ApiKey? = lock.withLock {
        keys[key] ?: loadApiKey(key)
    }

    private fun loadApiKey(key: String): ApiKey? {
        val file = keysDir.resolve(key).resolve("api-key.json")
        return if (file.isFile) ApiKey.read(file) else null
    }

    override fun init(filterConfig: FilterConfig) {
    }

    override fun destroy() {
    }

    override fun doFilter(request: ServletRequest, response: ServletResponse, chain: FilterChain) {
        if (response !is HttpServletResponse) return
        if (request !is HttpServletRequest) return

        fun sendError() {
            response.status = HttpServletResponse.SC_FORBIDDEN
            response.contentType = "text/plain"
            response.characterEncoding = "UTF-8"
            val bytes = """
                Access to the specified resource has been forbidden.
                Use PP-API-KEY http header.
            """.toByteArray()
            response.setContentLength(bytes.size)
            response.outputStream.write(bytes)
        }

        val headerKey = request.getHeader("PP-API-KEY") ?: return sendError()
        val apiKey = get(headerKey) ?: return sendError()
        chain.doFilter(RequestWrapper(request, apiKey), response)
    }

    private class RequestWrapper(request: HttpServletRequest, val apiKey: ApiKey) :
        HttpServletRequestWrapper(request) {

        private val principal: Principal by lazy { Principal { apiKey.key } }

        override fun getUserPrincipal(): Principal = principal
        override fun getRemoteUser(): String = principal.name
        override fun isUserInRole(role: String): Boolean = role in apiKey.roles
    }
}

private data class ApiKey(val key: String, val roles: Set<String>) {
    companion object {
        fun read(file: File): ApiKey =
            read(file.parentFile.name, Json.createReader(file.reader()).use { it.readObject() })

        fun read(key: String, json: JsonObject): ApiKey =
            ApiKey(
                key = key,
                roles = (json["roles"] as? JsonArray)
                    ?.filterIsInstance<JsonString>()
                    ?.map { it.string }
                    ?.toSet()
                        ?: emptySet()
            )
    }
}