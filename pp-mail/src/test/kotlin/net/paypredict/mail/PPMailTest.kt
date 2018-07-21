package net.paypredict.mail

import java.util.*
import javax.mail.Message
import javax.mail.Session
import javax.mail.Transport
import javax.mail.internet.InternetAddress
import javax.mail.internet.MimeMessage

/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 7/22/2018.
 */
object PPMailTest {

    @JvmStatic
    fun main(args: Array<String>) {
        val getMailSession: Session = Session.getDefaultInstance(PPMailConf.sessionProps(), null)
        val generateMailMessage = MimeMessage(getMailSession)
        generateMailMessage.addRecipient(Message.RecipientType.TO, InternetAddress(args[0]))
        generateMailMessage.subject = "PPMailTest"
        generateMailMessage.setContent(
            """PPMailTest ${Date()}""",
            "text/html"
        )
        generateMailMessage.setFrom(PPMailConf.from)
        val transport: Transport = getMailSession.getTransport("smtp")
        transport.connect(
            PPMailConf.host,
            PPMailConf.user,
            PPMailConf.password
        )
        transport.sendMessage(generateMailMessage, generateMailMessage.allRecipients)
        transport.close()
    }

}