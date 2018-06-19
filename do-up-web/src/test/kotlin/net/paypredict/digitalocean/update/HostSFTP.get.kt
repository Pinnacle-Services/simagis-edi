package net.paypredict.digitalocean.update

/**
 *
 *
 * Created by alexei.vylegzhanin@gmail.com on 6/17/2018.
 */

fun main(args: Array<String>) {
    HostSFTP.new().session {
        get(args[0])
            .reader()
            .use { it.readLines() }
            .forEach {
                println(it)
            }
    }
}