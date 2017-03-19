package com.simagis.claims.rest.api

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/16/2017.
 */
class ClaimDbApiException : RuntimeException {
    constructor() : super()
    constructor(message: String) : super(message)
    constructor(message: String?, cause: Throwable?) : super(message, cause)
    constructor(cause: Throwable?) : super(cause)
    constructor(message: String?, cause: Throwable?, enableSuppression: Boolean, writableStackTrace: Boolean) : super(message, cause, enableSuppression, writableStackTrace)
}