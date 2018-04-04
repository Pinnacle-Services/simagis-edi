package com.simagis.claims

import javax.servlet.annotation.WebServlet
import javax.servlet.http.HttpServlet

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 3/31/2018.
 */
@WebServlet(name = "ClaimDb Startup Servlet", loadOnStartup = 0)
class ClaimDbStartup : HttpServlet() {
    override fun init() {
        val contextPath = servletContext.contextPath
        if (contextPath.isNotEmpty()) {
            System.setProperty("paypredict.client", contextPath.removePrefix("/"))
        }
    }
}