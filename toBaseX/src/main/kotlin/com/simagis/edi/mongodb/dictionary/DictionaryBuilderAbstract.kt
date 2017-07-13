package com.simagis.edi.mongodb.dictionary

import org.bson.Document

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 7/14/2017.
 */
open class DictionaryBuilderAbstract(val options: Document) : DictionaryBuilder {
    protected lateinit var context: DictionaryContext
    override fun init(context: DictionaryContext) {
        this.context = context
    }

    override fun collect(claim: Document) {
    }

    override fun save() {

    }
}