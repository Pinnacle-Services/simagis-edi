package com.simagis.edi.rserve

fun main(args: Array<String>) {
    val r = RServe(debugR = true)
    for (i in 1..1000) {
        r exec """
            # R Script:
            meanVal = mean(c(1,2,3,4))
            """
        val meanVal = r eval "meanVal"
        print("${meanVal.asDouble()} ")
    }
    r.shutdown()
}