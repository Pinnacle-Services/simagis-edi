package com.simagis.edi.mongodb.dictionary

import org.bson.Document

class DictionaryBuilder_cpt(options: Document): DictionaryBuilderAbstractCollect(options) {
    private lateinit var codes: CodesMap

    override fun init(context: DictionaryContext) {
        super.init(context)
        codes = javaClass.readCodesMap("claim835-cpt-codes.json", {
            it.getString("cpt_code") to it.getString("short_description")
        })
    }

    override fun collect(claim: Document) {
        claim.opt<List<*>>("svc")?.forEach {
            it.doc?.opt<String>("cptId")?.let {
                collectByValue(it, codes.toItemBuilder())
            }
        }
    }
}
