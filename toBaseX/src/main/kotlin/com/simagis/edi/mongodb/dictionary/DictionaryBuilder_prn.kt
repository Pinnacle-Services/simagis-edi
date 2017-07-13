package com.simagis.edi.mongodb.dictionary

import org.bson.Document

class DictionaryBuilder_prn(options: Document): DictionaryBuilderAbstractCollect(options) {

    override fun collect(claim: Document) {
        claim.opt<String>("prn")?.let {
            collectByValue(it)
        }
    }
}
