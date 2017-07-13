package com.simagis.edi.mongodb

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 7/14/2017.
 */
fun main(args: Array<String>) {
    ImportJob.open(args)
    info("starting job", detailsJson = ImportJob.jobDoc)

    val dictionaries = ImportJob.options.rebuildDicts.dictionaries
    dictionaries.forEach {
        it.builder.init(it)
    }

    ImportJob.options.claimTypes["835c"].targetCollection.find().forEach { claim ->
        dictionaries.forEach {
            it.builder.collect(claim)
        }
    }

    dictionaries.forEach {
        it.builder.save()
    }
}
