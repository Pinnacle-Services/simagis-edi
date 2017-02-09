package com.simagis.edi.basex

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.StringReader
import java.util.*
import javax.xml.transform.TransformerException

/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 2/7/2017.
 */
class ISATest {
    companion object {
        const val TEST1
                = "ISA*00*          *00*          *ZZ*SENDER_NAME    *ZZ*PAYER_NAME     *160923*1607*{*00501*875381672*0*P*:~" +
                "GS*HP*CWI00001*PROVIDERHIS*20160923*1607*1*X*005010X221A1~" +
                "ST*835*00000445~" +
                "BPR*H*0*C*NON************20160923~" +
                "TRN*1*2016012302000111*1234567890~" +
                "N1*PR*Any Company,  Inc.~" +
                "N3*PO Box 5555~" +
                "N4*Any*SC*123456789~" +
                "REF*2U*NON~" +
                "PER*BL*Customer Service*TE*8005555555~" +
                "N1*PE*ORGANIZATION NAME*FI*222333333~" +
                "LX*1~" +
                "CLP*LAM1234567890*1*58.5*0*5.83*13*300012345678~" +
                "NM1*QC*1*ANY*NAME~" +
                "NM1*IL*1*ANY*NAME*D***MI*34567890~" +
                "SVC*HC:55555*58.5*0~" +
                "DTM*150*20160824~" +
                "CAS*PR*1*5.83~" +
                "CAS*CO*45*52.67~" +
                "AMT*B6*5.83~" +
                "SE*19*00000445~" +
                "GE*1*1~" +
                "IEA*1*875381672~"

        const val TEST_ERR = "ERR" + TEST1

        fun toRandomFormatted(text: String): String {
            val formatted = mutableListOf<String>().apply {
                val random = Random(157)
                val split = text.split('~')
                this += split.first()
                for (i in 1..split.size - 1) {
                    val randomSpace = ByteArray(random.nextInt(8)) { random.nextInt(32).toByte() }
                    this += randomSpace.toString(ISA.CHARSET) + split[i]
                }
            }.joinToString(separator = "~")
            return formatted
        }
    }

    @Test
    fun parse() {
        val isa = ISA.of(TEST1)
        assertEquals(isa.name, "835-201609")
        assertEquals(isa.stat.toString(), "Stat(status=VALID, doc=Doc(type=835, date=20160923, time=1607), counts=Counts(clp=1), error=null)")
        assertEquals(isa.toXML().size, 3128)
        assertTrue("xml contains Control=\"875381672\"", isa.toXML().toString(ISA.CHARSET).contains("Control=\"875381672\""))
    }

    @Test(expected = TransformerException::class)
    fun parseError() {
        ISA.of(TEST_ERR).toXML()
    }

    @Test
    fun filter() {
        assertEquals(
                ISA.filter(StringReader(toRandomFormatted(TEST1)), '~', StringBuilder()).toString(),
                TEST1)
    }
}