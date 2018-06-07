package com.simagis.claims

import javax.servlet.ServletContextEvent
import javax.servlet.ServletContextListener
import javax.servlet.annotation.WebListener

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/31/2018.
 */
@WebListener
class ClaimDbStartup : ServletContextListener {
    override fun contextInitialized(sce: ServletContextEvent) {
        clientName = sce.servletContext.contextPath.let {
            when (it) {
                "" -> "ROOT"
                else -> it.removePrefix("/")
            }
        }.also { sce.servletContext.log("clientName initialized as $it") }
    }

    override fun contextDestroyed(sce: ServletContextEvent) = Unit
}

var clientName: String = "ROOT"
    private set(value) {
        field = value
    }

val clientRoot: String by lazy {
    clientName.let {
        when (it) {
            "ROOT" -> "/"
            else -> "/$it/"
        }
    }
}