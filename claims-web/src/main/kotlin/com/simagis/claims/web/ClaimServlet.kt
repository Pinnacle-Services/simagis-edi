package com.simagis.claims.web

import com.mongodb.MongoClient
import org.bson.Document
import org.bson.json.JsonWriterSettings
import java.net.HttpURLConnection.HTTP_NOT_FOUND
import java.net.HttpURLConnection.HTTP_OK
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

    private val mongoHost = System.getProperty("claims.mongo.host", "localhost")
    private val mongoDB = System.getProperty("claims.mongo.db", "claims")

    override fun doGet(request: HttpServletRequest, response: HttpServletResponse) {
        val path = request.pathInfo.split('/')
        if (path.size != 3) {
            response.status = HTTP_NOT_FOUND
            return
        }
        val collectionName = "claims_${path[1]}"
        val documentId = path[2]

        val mongoClient = MongoClient(mongoHost)
        val db = mongoClient.getDatabase(mongoDB)
        val collection = db.getCollection(collectionName)
        val document: Document? = collection.find(Document("_id", documentId)).first()

        if (document == null) {
            response.status = HTTP_NOT_FOUND
            return
        }

        response.status = HTTP_OK
        response.writer.print(document.toJson(JsonWriterSettings(true)))
    }
}
