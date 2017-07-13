package com.simagis.edi.mongodb.dictionary

import org.bson.Document

class DictionaryBuilder_frmn(options: Document): DictionaryBuilderAbstractCollect(options) {

    override fun collect(claim: Document) {
        claim.opt<String>("frmn")?.let {
            collectByValue(it)
        }
    }
}
