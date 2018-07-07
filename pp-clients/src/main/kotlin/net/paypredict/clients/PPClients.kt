package net.paypredict.clients

import java.io.File
import javax.json.Json
import javax.json.JsonNumber
import javax.json.JsonObject
import javax.json.JsonString

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


private val clientsDir = File("/PayPredict/clients")


