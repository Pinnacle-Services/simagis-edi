package com.simagis.claims.web

import com.mongodb.MongoClient
import com.mongodb.client.FindIterable
import com.mongodb.client.MongoCollection
import com.simagis.claims.rest.api.ClaimDb
import com.simagis.claims.web.ui.ClaimQuery
import com.simagis.claims.web.ui.applyParameters
import com.simagis.claims.web.ui.toClaimQuery
import com.simagis.edi.mdb.MDBCredentials
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import org.bson.Document
import java.net.HttpURLConnection.HTTP_NOT_FOUND
import java.net.HttpURLConnection.HTTP_OK
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import javax.json.Json
import javax.json.JsonArray
import javax.json.JsonNumber
import javax.json.JsonObject
import javax.servlet.annotation.WebServlet
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 3/10/2017.
 */
@WebServlet(name = "ClaimServlet", urlPatterns = arrayOf("/claim/*", "/query/*"))
class ClaimServlet : HttpServlet() {
    override fun doPost(request: HttpServletRequest, response: HttpServletResponse) {
        response.status = HTTP_NOT_FOUND
    }

    private val claimsDbName = System.getProperty("claims.mongo.db", "claims")
    private val claimsADbName = System.getProperty("claims.mongo.adb", "claimsA")
    private val decoder: Base64.Decoder = Base64.getUrlDecoder()
    private val mongoClient: MongoClient by lazy { MDBCredentials.mongoClient(ClaimDb.server) }
    private val claimsDb = mongoClient.getDatabase(claimsDbName)
    private val claimsADb = mongoClient.getDatabase(claimsADbName)
    private val claimsCollectionPrefix = System.getProperty("claims.mongo.db.claimsPrefix", "claims_")


    override fun doGet(request: HttpServletRequest, response: HttpServletResponse) {
        val servletPath = request.servletPath
        val queryString = request.queryString ?: ""
        val paging = Paging.of(request.parameterMap)
        val errors = StringBuilder()

        var cq: ClaimQuery? = null
        val documents: FindIterable<Document>? = (if (servletPath == "/query")
            query(request, paging, errors) { cq = this } else
            find(request, paging))

        if (documents == null) {
            response.send(HTTP_NOT_FOUND, errors.toString().toByteArray(), "text/plain")
            return
        }

        val html = Claims835ToHtml(
                db = claimsDb,
                cq = cq,
                paging = paging,
                root = servletPath + request.pathInfo,
                queryString = queryString)
                .apply {
                    documents.forEach { document ->
                        if (!append(document)) return@apply
                    }
                }

        response.send(HTTP_OK, html.toBytes())
    }

    private fun HttpServletResponse.send(httpStatus: Int, content: ByteArray, contentType: String = "text/html") {
        this.status = httpStatus
        this.contentType = contentType
        this.characterEncoding = "UTF-8"
        this.setContentLength(content.size)
        this.outputStream.write(content)
    }

    private fun find(request: HttpServletRequest, paging: Paging): FindIterable<Document>? {
        val pathInfo = request.pathInfo
        val path = pathInfo.split('/')
        if (path.size != 3) {
            return null
        }
        val collection = getClaimsCollection(path[1])
        val id = path[2]

        fun find(json: JsonArray): FindIterable<Document> {
            fun JsonObject.toDocument(): Document = Document.parse((this).toString())
            val filter = (json[0] as JsonObject).toDocument()
            if (paging.isPageable) {
                paging.found = collection.count(filter)
            }
            return collection.find(filter).apply {
                for (i in 1..json.size - 1) {
                    when (i) {
                        1 -> projection((json[i] as JsonObject).toDocument())
                        2 -> sort((json[i] as JsonObject).toDocument())
                        3 -> skip((json[i] as JsonNumber).intValue())
                        4 -> limit((json[i] as JsonNumber).intValue())
                    }
                }
                if (paging.isPageable) {
                    skip((paging.ps * paging.pn).toInt())
                    limit(paging.ps.toInt())
                }
            }
        }

        fun String.decode(): String = decoder.decode(this).toString(Charsets.UTF_8)

        return when {
            id.startsWith("{") -> collection.find(Document.parse(id))
            id.startsWith("[") -> find(Json.createReader(id.reader()).readArray())
            id.startsWith("=") -> find(Json.createReader(id.substring(1).decode().reader()).readArray())
            id.contains("-R-") -> collection.find(Document("_id", id))
            else -> collection.find(Document("acn", id))
        }
    }

    private val queryCountCache: MutableMap<Document, Long> = ConcurrentHashMap()
    private fun query(request: HttpServletRequest,
                      paging: Paging,
                      errors: StringBuilder,
                      onNewCQ: ClaimQuery.() -> Unit)
            : FindIterable<Document>? {
        val path = request.pathInfo.removePrefix("/")
        if (path.isBlank()) return null
        val cq = ClaimDb.cq.find(doc { `+`("path", path) }).let {
            when (it.count()) {
                1 -> it.first()?.toClaimQuery()?.apply { onNewCQ() }
                0 -> null
                else -> {
                    errors.append("too many cq path '$path' found:\n")
                    errors.append(it.joinToString(separator = "\n\n") { it.toJson() })
                    null
                }
            }
        } ?: return null

        val collection = getClaimsCollection(cq.type)
        val filter = Document.parse(cq.find).applyParameters { name -> request.getParameter(name) }
        paging.found = queryCountCache.computeIfAbsent(filter) { collection.count(it) }
        return collection
                .find(filter)
                .projection(Document.parse(cq.projection))
                .sort(Document.parse(cq.sort))
                .apply {
                    if (paging.isPageable) {
                        skip((paging.ps * paging.pn).toInt())
                        limit(paging.ps.toInt())
                    } else {
                        paging.ps = cq.pageSize.toLong()
                        limit(paging.ps.toInt())
                    }
                }
    }

    private fun getClaimsCollection(type: String): MongoCollection<Document> = when(type.lastOrNull()?.toUpperCase()) {
        'A' -> claimsADb.getCollection("$claimsCollectionPrefix$type")
        else -> claimsDb.getCollection("$claimsCollectionPrefix$type")
    }
}

