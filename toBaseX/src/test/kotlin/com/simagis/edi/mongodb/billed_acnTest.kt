package com.simagis.edi.mongodb

import org.junit.Assert.assertEquals
import org.junit.Test

/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 9/8/2017.
 */
class billed_acnTest {
    @Test
    fun acn_to_id() {
        assertEquals("123",
                ImportJob.billed_acn.acn_to_id("123Z00")
        )
        assertEquals(null,
                ImportJob.billed_acn.acn_to_id("123Z001")
        )
        assertEquals(null,
                ImportJob.billed_acn.acn_to_id("123")
        )
        assertEquals(null,
                ImportJob.billed_acn.acn_to_id("Z00")
        )
    }

}