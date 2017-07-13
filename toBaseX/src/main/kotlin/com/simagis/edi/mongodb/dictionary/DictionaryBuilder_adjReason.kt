package com.simagis.edi.mongodb.dictionary

import org.bson.Document

class DictionaryBuilder_adjReason(options: Document): DictionaryBuilderAbstractCollect(options) {

    override fun collect(claim: Document) {
        claim.opt<List<*>>("svc")?.forEach {
            it.doc?.opt<List<*>>("adj")?.forEach {
                it.doc?.opt<String>("adjReason")?.let {
                    collectByValue(it)
                }
            }
        }
    }
}

