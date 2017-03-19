package com.simagis.claims.rest.api.jobs

import com.simagis.claims.rest.api.ClaimDb

/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 3/20/2017.
 */
fun main(args: Array<String>) {
    Import.start(onDone = {
        ClaimDb.shutdown()
    })
}