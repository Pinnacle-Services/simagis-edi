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

val localClientDir: File by lazy { clientsDir.resolve("claims").also { it.mkdir() } }
val localClientTmpDir: File by lazy { localClientDir.resolve(".tmp").also { it.mkdir() } }
val localClientImageDir: File by lazy { localClientDir.resolve("image").also { it.mkdir() } }