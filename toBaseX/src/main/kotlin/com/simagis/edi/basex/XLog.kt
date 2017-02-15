package com.simagis.edi.basex

interface XLog {
    fun trace(
            message: String,
            action: String? = null,
            details: String? = null,
            detailsXml: String? = null)

    fun info(
            message: String,
            action: String? = null,
            details: String? = null,
            detailsXml: String? = null)

    fun warning(
            message: String,
            exception: Throwable? = null,
            action: String? = null,
            details: String? = null,
            detailsXml: String? = null)

    fun error(
            message: String,
            exception: Throwable? = null,
            action: String? = null,
            details: String? = null,
            detailsXml: String? = null)
}