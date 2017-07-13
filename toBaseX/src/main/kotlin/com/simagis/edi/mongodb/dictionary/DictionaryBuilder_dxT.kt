package com.simagis.edi.mongodb.dictionary

import org.bson.Document

class DictionaryBuilder_dxT(options: Document): DictionaryBuilderAbstract(options) {

    override fun save() {
        val codes= javaClass.readCodesMap("dxT-codes.json", {
            it.getString("id") to it.getString("caption")
        })
        context.collection.replaceAll(codes)
    }
}

