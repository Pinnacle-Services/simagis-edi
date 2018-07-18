package com.simagis.claims.web

import org.bson.Document
import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonString

typealias Dir = Map<String, String>

val cptCodes: Dir by lazy {
    loadAsMap("claim835-cpt-codes.json", {
        it.getString("cpt_code") to it.getString("short_description")
    })
}
val statusCodes: Dir by lazy {
    loadAsMap("claim835-status-codes.json", {
        it.getString("id") to it.getString("caption")
    })
}
val adjGrpCodes: Dir by lazy {
    loadAsMap("claim835-adjGrp-codes.json", {
        it.getString("id") to it.getString("caption")
    })
}
val adjReasonCodes: Dir by lazy {
    loadAsMap("claim835-adjReason-codes.json", { json ->
        val reason = json.getString("Reason")
        val description = json["Description"]
        if (reason is String && description is JsonString)
            reason to description.string else null
    })
}
val remCodes: Dir by lazy {
    loadAsMap("claim835-rem-codes.json", {
        it.getString("Remark") to it.getString("Description")
    })
}
val icd10Codes: Dir by lazy {
    loadAsMap("icd10-codes.json", {
        it.getString("icd10_code_id") to it.getString("description")
    })
}

val dxT: Dir by lazy {
    loadAsMap("dxT-codes.json", {
        it.getString("id") to it.getString("caption")
    })
}

val codes: Map<String, Dir> by lazy {
    mapOf(
        "cpt" to cptCodes,
        "status" to statusCodes,
        "adjGrp" to adjGrpCodes,
        "adjReason" to adjReasonCodes,
        "rem" to remCodes,
        "icd10" to icd10Codes,
        "dxT" to dxT
    )
}

class Keys(private val order: List<String>, val map: Dir) {
    fun orderedKeys(claim: Document): List<String> = mutableListOf<String>().apply {
        val inst = claim.keys.sorted()
        this += order.filter { inst.contains(it) }
        this += inst.filter { !order.contains(it) }
    }

}

val keys: Keys by lazy {
    val order = mutableListOf<String>()
    val map = loadAsMap("claim835.keyNames.json", {
        val key = it.getString("key")
        order += key
        key to it.getString("name")
    })
    Keys(order, map)
}

private fun loadAsMap(name: String, map: (JsonObject) -> Pair<String, String>?): Dir =
    mutableMapOf<String, String>().apply {
        Claims835ToHtml::class.java.getResourceAsStream(name).use { Json.createReader(it).readArray() }
            .forEach {
                if (it is JsonObject) {
                    map(it)?.let { this += it }
                }
            }
    }

