package net.paypredict.digitalocean

import java.net.URL

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 6/16/2018.
 */

object DigitalOceanMetadata {
    val id: String by lazy { metadata("id") }
    val hostname: String by lazy { metadata("hostname") }
    val tags: Set<String> by lazy { metadata("tags").lines().toSet() }
}

private fun metadata(name: String): String =
    System.getenv("PP_DO_$name".toUpperCase())
            ?: URL("http://169.254.169.254/metadata/v1/$name")
                .openConnection()
                .getInputStream()
                .reader()
                .use { it.readText() }