package com.simagis.edi.mongodb.dictionary

import org.bson.Document

class DictionaryBuilder_dxV(options: Document) : DictionaryBuilderAbstractCollect(options) {
    private lateinit var codes: CodesMap

    override fun init(context: DictionaryContext) {
        super.init(context)
        codes = javaClass.readCodesMap("icd10-codes.json", {
            it.getString("icd10_code_id") to it.getString("description")
        })
    }

    override fun collect(claim: Document) {
        claim.opt<List<*>>("dx")?.forEach {
            it.doc?.opt<String>("dxV")?.let {
                collectByValue(it) { id ->
                    codes[id]?.let { dsc ->
                        DictionaryItem(id, dsc, "$id-$dsc", true)
                    } ?: id.toDictionaryItem(true)
                }
            }
        }
    }
}
