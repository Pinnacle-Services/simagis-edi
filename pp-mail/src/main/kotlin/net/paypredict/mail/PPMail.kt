package net.paypredict.mail

import java.io.File
import java.util.*
import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonString
import javax.mail.internet.InternetAddress

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 7/22/2018.
 */
object PPMailConf {

    val host: String by lazy { conf.getString("host", "admin") }
    val user: String by lazy { conf.getString("user", "admin") }
    val password: String by lazy { conf.getString("password", "admin") }

    val from: InternetAddress by lazy {
        InternetAddress(conf.getString("from", "do-not-replay@paypredict.net"))
    }

    fun sessionProps(): Properties =
        Properties().apply {
            val session = conf["session"] as? JsonObject ?: emptyJson
            session.forEach { key, value ->
                if (value is JsonString) {
                    this[key] = value.string
                }
            }
        }

    private val confFile = File("/PayPredict/conf/pp-mail.json")
    private val emptyJson: JsonObject by lazy { Json.createObjectBuilder().build() }
    private val conf: JsonObject by lazy {
        if (confFile.isFile)
            Json.createReader(confFile.reader()).use { it.readObject() } else
            emptyJson
    }
}