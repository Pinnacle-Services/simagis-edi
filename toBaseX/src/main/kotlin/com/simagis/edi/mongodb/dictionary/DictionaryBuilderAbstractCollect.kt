package com.simagis.edi.mongodb.dictionary

import org.bson.Document

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 7/14/2017.
 */
open class DictionaryBuilderAbstractCollect(options: Document) : DictionaryBuilderAbstract(options) {
    protected lateinit var items: Map<String, DictionaryItem>
    protected val toInsert = mutableMapOf<String, DictionaryItem>()
    protected val toUpdate = mutableMapOf<String, DictionaryItem>()

    protected fun collectByValue(id: String, toItem: (String) -> DictionaryItem = { it.toDictionaryItem(true) }) {
        val old = items[id]
        toItem(id).let { new ->
            when {
                old == null -> toInsert[new._id] = new
                old.inUse != new.inUse -> toUpdate[new._id] = new
            }
        }
    }

    override fun init(context: DictionaryContext) {
        super.init(context)
        this.items = mutableMapOf<String, DictionaryItem>().apply {
            context.collection.find().forEach {
                it.toItem()?.let { this[it._id] = it }
            }
        }
    }

    override fun save() {
        context.collection.insertAll(toInsert.values.toList())
        context.collection.updateInUse(toUpdate.values.toList())
    }
}