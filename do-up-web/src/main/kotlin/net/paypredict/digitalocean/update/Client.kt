package net.paypredict.digitalocean.update

import net.paypredict.digitalocean.DigitalOceanMetadata
import java.io.File

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 6/17/2018.
 */

val tagsClient: String by lazy { DigitalOceanMetadata.tags
    .firstOrNull { it.startsWith("pp-") }
    ?.removePrefix("pp-")
    ?: throw DigitalOceanUpdateException("DigitalOcean tag pp-* not found")
}

val tagsClientDir: File by lazy { clientsDir.resolve(tagsClient).also { it.mkdir() } }