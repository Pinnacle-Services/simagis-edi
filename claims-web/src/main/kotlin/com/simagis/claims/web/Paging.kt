package com.simagis.claims.web

class Paging(val ps: Long, val pn: Long) {
    val isPageable = ps > 0
    var found: Long = 0
    val pageCount get() = (found + ps - 1) / ps

    companion object {
       fun of(parameters: Map<String, Array<String>>): Paging {
            val ps = parameters["ps"]?.firstOrNull()?.let { if (it.isNotEmpty()) it.toLong() else 100 } ?: 0
            val pn = parameters["pn"]?.firstOrNull()?.let { if (it.isNotEmpty()) it.toLong() else 0 } ?: 0
            return Paging(ps, pn)
        }
    }
}