package net.paypredict.digitalocean.update

import net.paypredict.digitalocean.DigitalOceanMetadata
import java.io.File

/**
 * <p>
 * Created by alexei.vylegzhanin@gmail.com on 6/17/2018.
 */
val ppDir: File by lazy { File("/PayPredict").absoluteFile }
val confDir: File by lazy { ppDir.resolve("conf").absoluteFile }
val clientsDir: File by lazy { ppDir.resolve("clients").absoluteFile }
