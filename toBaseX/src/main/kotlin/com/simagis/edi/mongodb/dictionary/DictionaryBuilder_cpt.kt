package com.simagis.edi.mongodb.dictionary

import org.bson.Document

class DictionaryBuilder_cpt(options: Document): DictionaryBuilderAbstractCollect(options) {

    override fun collect(claim: Document) {
        claim.opt<List<*>>("svc")?.forEach {
            it.doc?.opt<String>("cptId")?.let {
                collectByValue(it)
            }
        }
    }
}
