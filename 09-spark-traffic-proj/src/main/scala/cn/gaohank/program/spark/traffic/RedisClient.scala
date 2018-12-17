package cn.gaohank.program.spark.traffic

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisClient extends Serializable {
    val redisHost = "hadoop"
    val redisPort = 12002
    val redisTimeOut = 30000
    lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeOut)
    lazy val hook = new Thread{
        override def run() : Unit = {
            println("Execute hook thread: " + this)
            pool.destroy()
        }
    }

    sys.addShutdownHook(hook.run())
}
