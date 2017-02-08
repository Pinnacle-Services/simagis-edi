package com.simagis.edi.basex

import com.berryworks.edireader.EDISyntaxException
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.io.File

/**
 *
 * Created by alexei.vylegzhanin@gmail.com on 2/6/2017.
 */
class XRegTest {
    lateinit var test1: File
    lateinit var test2: File

    @Before
    fun setUp() {
        test1 = toFile(ISATest.TEST1)
        test2 = toFile(ISATest.TEST2)
    }

    private fun toFile(text: String): File = File.createTempFile("test-", ".bin").apply { writeText(text) }

    @After
    fun tearDown() {
        test1.delete()
        test2.delete()
    }

    @Test
    fun loading() {
        XReg().newSession {
            withFile(test1) {
                split().forEach { isa ->
                    withISA(isa) {
                        updateIsaStatus("READY")
                        xLog.info("ISA: ${isa.name}",
                                details = isa.code,
                                detailsXml = isa.toXML().toString(ISA.CHARSET)
                        )
                    }
                }
                updateFileStatus("READY")
                xLog.info("File: $test1")
            }
        }
    }

    @Test(expected = EDISyntaxException::class)
    fun loadingError() {
        XReg().newSession {
            withFile(test2) {
                split()
            }
        }
    }
}