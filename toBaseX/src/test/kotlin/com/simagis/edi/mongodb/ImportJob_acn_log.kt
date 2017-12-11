package com.simagis.edi.mongodb

import java.util.logging.Logger

/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 12/12/2017.
 */
fun main(args: Array<String>) {
    val log = Logger.getGlobal()
    log.info("ImportJob.acn_log.tree: loading...")
    with(ImportJob.acn_log.db) {
        log.info("with(ImportJob.acn_log.tree):")
        log.info("accns.size = ${accns.size}")
        log.info("files.size = ${files.size}")
    }
}