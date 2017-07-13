package com.simagis.edi.mongodb.dictionary

import org.bson.Document

class DictionaryBuilder_rem(options: Document): DictionaryBuilderAbstract(options) {

    override fun save() {
        val codes= javaClass.readCodesMap("claim835-rem-codes.json", {
            it.getString("Remark") to it.getString("Description")
        })
        context.collection.replaceAll(codes)
    }
}

