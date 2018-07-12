package net.paypredict.clients

import java.io.File
import java.io.StringWriter
import javax.json.*
import javax.json.stream.JsonGenerator

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 7/7/2018.
 */

class PPClient(
    val id: String,
    val conf: JsonObject
) {
    val mongo: Conf.Mongo by lazy {
        val mongo = conf["mongo"] as? JsonObject
        Conf.Mongo(
            host = (mongo?.get("host") as? JsonString)?.string ?: "127.0.0.1",
            port = (mongo?.get("port") as? JsonNumber)?.intValue() ?: -1
        )
    }
}

class Conf {
    data class Mongo(
        val host: String = "127.0.0.1",
        val port: Int = -1
    )
}

class PPClients {
    val all: List<PPClient> by lazy {
        clientsDir
            .listFiles { pathname -> pathname.isDirectory }
            ?.mapNotNull { clientDir ->
                val confFile = clientDir
                    .resolve("conf")
                    .resolve("claims-db.json")
                if (confFile.isFile)
                    PPClient(
                        id = clientDir.name,
                        conf = Json.createReader(confFile.reader()).use { it.readObject() })
                else
                    null
            }
                ?: emptyList()
    }
}

fun ppClients(): PPClients = PPClients()


internal val clientsDir = File("/PayPredict/clients")


private val jsonPP: JsonWriterFactory by lazy {
    Json.createWriterFactory(mapOf<String, Any>(JsonGenerator.PRETTY_PRINTING to true))
}

fun JsonObject.toStringPP(): String =
    StringWriter().use { jsonPP.createWriter(it).write(this); it }.toString().trimStart()

operator fun JsonValue?.get(vararg path: String): JsonValue? {
    var result: JsonValue = this ?: return null
    for (key in path) {
        if (result is JsonObject)
            result = result[key] ?: return null else
            return null
    }
    return result
}