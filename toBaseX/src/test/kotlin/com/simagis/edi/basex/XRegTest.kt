package com.simagis.edi.basex

import org.junit.Test

import org.junit.Assert.*
import java.io.File

/**
 *
 * Created by alexei.vylegzhanin@gmail.com on 2/6/2017.
 */
class XRegTest {
    @Test
    fun logging() {
        XReg().use { xReg ->
            File.createTempFile("test-", ".bin").let { tempFile ->
                try {
                    tempFile.writeText("Проверка")
                    xReg.file = XReg.DBFile(tempFile)
                    xReg.log.info("Проверка")
                } finally {
                    tempFile.delete()
                }
            }
        }
    }
}