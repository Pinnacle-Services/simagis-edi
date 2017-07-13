package com.simagis.edi.mongodb.dictionary

import org.bson.Document

class DictionaryBuilder_adjGrp(options: Document): DictionaryBuilderAbstract(options) {

    override fun save() {
        val codes= javaClass.readCodesMap("claim835-adjGrp-codes.json", {
            it.getString("id") to it.getString("caption")
        })
        context.collection.replaceAll(codes)
    }
}
