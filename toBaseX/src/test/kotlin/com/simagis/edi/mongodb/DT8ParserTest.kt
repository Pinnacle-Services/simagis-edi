package com.simagis.edi.mongodb

import org.junit.Test

import org.junit.Assert.*
import java.time.Instant

/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 12/16/2017.
 */
class DT8ParserTest {
    @Test
    fun parse() {
        assertEquals(Instant.parse("2017-01-01T12:00:00.000Z"), parseDT8("2017")?.toInstant())
        assertEquals(Instant.parse("2017-11-01T12:00:00.000Z"), parseDT8("201711")?.toInstant())
        assertEquals(Instant.parse("2017-11-30T12:00:00.000Z"), parseDT8("20171130")?.toInstant())
    }
}