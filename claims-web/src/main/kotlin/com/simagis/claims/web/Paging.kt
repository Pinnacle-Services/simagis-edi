package com.simagis.claims.web

class Paging(val ps: Int, val pn: Int) {
    val isPageable = ps > 0
    var found: Int = 0
    val pageCount get() = (found + ps - 1) / ps

    companion object {
       fun of(parameters: Map<String, Array<String>>): Paging {
            val ps = parameters["ps"]?.firstOrNull()?.let { if (it.isNotEmpty()) it.toInt() else 100 } ?: 0
            val pn = parameters["pn"]?.firstOrNull()?.let { if (it.isNotEmpty()) it.toInt() else 0 } ?: 0
            return Paging(ps, pn)
        }
    }
}