package com.simagis.claims.web

import com.mongodb.MongoClient
import com.mongodb.client.FindIterable
import com.simagis.claims.rest.api.ClaimDb
import com.simagis.claims.web.ui.applyParameters
import com.simagis.claims.web.ui.toClaimQuery
import com.simagis.edi.mdb.MDBCredentials
import com.simagis.edi.mdb.`+`
import com.simagis.edi.mdb.doc
import org.bson.Document
import java.net.HttpURLConnection.HTTP_NOT_FOUND
import java.net.HttpURLConnection.HTTP_OK
import java.util.*
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

    private val mongoHost = System.getProperty("claims.mongo.host", "127.0.0.1")
    private val mongoDB = System.getProperty("claims.mongo.db", "claims")
    private val decoder: Base64.Decoder = Base64.getUrlDecoder()
    private val mongoClient: MongoClient by lazy { MDBCredentials.mongoClient(mongoHost) }
    private val db = mongoClient.getDatabase(mongoDB)

    override fun doGet(request: HttpServletRequest, response: HttpServletResponse) {
        val servletPath = request.servletPath
        val queryString = request.queryString ?: ""
        val paging = Paging.of(request.parameterMap)

        val documents: FindIterable<Document> = (if (servletPath == "/query")
            query(request, paging) else
            find(request, paging))
                ?: let { response.status = HTTP_NOT_FOUND; return }

        val html = Claims835ToHtml(
                db = db,
                paging = paging,
                root = servletPath + request.pathInfo,
                queryString = queryString)
                .apply {
                    documents.forEach { document ->
                        if (!append(document)) return@apply
                    }
                }
        val bytes = html.toBytes()

        response.status = HTTP_OK
        response.contentType = "text/html"
        response.characterEncoding = "UTF-8"
        response.setContentLength(bytes.size)
        response.outputStream.write(bytes)
    }

    private fun find(request: HttpServletRequest, paging: Paging): FindIterable<Document>? {
        val pathInfo = request.pathInfo
        val path = pathInfo.split('/')
        if (path.size != 3) {
            return null
        }
        val collection = db.getCollection("claims_${path[1]}")
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

    private fun query(request: HttpServletRequest, paging: Paging): FindIterable<Document>? {
        val name = request.pathInfo
        val cq = ClaimDb.cq
                .find(doc { `+`("name", name) })
                .first()
                ?.toClaimQuery()
                ?: return null
        return db.getCollection("claims_${cq.type}")
                .find(Document.parse(cq.find).applyParameters({ name -> request.getParameter(name) }))
                .projection(Document.parse(cq.projection))
                .sort(Document.parse(cq.sort))
                .apply {
                    paging.found = count().toLong()
                    paging.ps = cq.pageSize.toLong()
                }
    }
}

