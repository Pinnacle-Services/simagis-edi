package com.simagis.claims.web

import com.mongodb.MongoClient
import com.mongodb.client.FindIterable
import org.bson.Document
import java.net.HttpURLConnection.HTTP_NOT_FOUND
import java.net.HttpURLConnection.HTTP_OK
import javax.json.Json
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
@WebServlet(name = "ClaimServlet", urlPatterns = arrayOf("/claim/*"))
class ClaimServlet : HttpServlet() {
    override fun doPost(request: HttpServletRequest, response: HttpServletResponse) {
        response.status = HTTP_NOT_FOUND
    }

    private val mongoHost = System.getProperty("claims.mongo.host", "127.0.0.1")
    private val mongoDB = System.getProperty("claims.mongo.db", "claims")

    override fun doGet(request: HttpServletRequest, response: HttpServletResponse) {
        val path = request.pathInfo.split('/')
        if (path.size != 3) {
            response.status = HTTP_NOT_FOUND
            return
        }
        val collectionName = "claims_${path[1]}"
        val id = path[2]

        val mongoClient = MongoClient(mongoHost)
        val db = mongoClient.getDatabase(mongoDB)
        val collection = db.getCollection(collectionName)

        val paging = Paging.of(request.parameterMap)

        val documents: FindIterable<Document> = when {
            id.startsWith("{") -> collection.find(Document.parse(id))
            id.startsWith("[") -> Json.createReader(id.reader()).readArray().let { json ->
                fun JsonObject.toDocument(): Document = Document.parse((this).toString())
                collection.find((json[0] as JsonObject).toDocument()).apply {
                    for (i in 1..json.size - 1) {
                        when(i) {
                            1 -> projection((json[i] as JsonObject).toDocument())
                            2 -> sort((json[i] as JsonObject).toDocument())
                            3 -> skip((json[i] as JsonNumber).intValue())
                            4 -> limit((json[i] as JsonNumber).intValue())
                        }
                    }
                    if (paging.isPageable) {
                        skip(paging.ps * paging.pn)
                        limit(paging.ps)
                    }
                }
            }
            id.contains("-R-") -> collection.find(Document("_id", id))
            else -> collection.find(Document("acn", id))
        }

        val html = Claims835ToHtml(paging = paging, root = request.servletPath + request.pathInfo).apply {
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
}
