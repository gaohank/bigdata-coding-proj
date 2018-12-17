package cn.gaohank.program.redis;

import redis.clients.jedis.*;
import redis.clients.util.Hashing;

import java.util.ArrayList;
import java.util.List;

public class RedisTools {

	public static int SECONDS = 3600 * 24;// 为key指定过期时间，单位是秒

	private static JedisPool pool;
	private static ShardedJedisPool shardPool;

	static {
		// jedis配置
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxIdle(1000);// 最大空闲
		config.setMaxTotal(10240);// 最大连接数
		if (pool == null) {// config：配置参数； 6379：默认端口号，可以更改；e_learning：密码
			pool = new JedisPool(config, "hadoop", 12002, 0);
		}
	}

	public static Jedis getJedis() {
		return pool.getResource();
	}

	public static void closeJedis(Jedis jedis) {
		pool.returnResource(jedis);
	}

	public static void createJedisShardPool() {
		JedisPoolConfig config = new JedisPoolConfig();// Jedis池配置
		config.setMaxTotal(10);
		config.setMaxIdle(1000);// 对象最大空闲时间
		config.setMaxWaitMillis(1000 * 10);// 获取对象时最大等待时间
		config.setMinIdle(0);// 对象最小空闲时间
		config.setTestOnBorrow(false);// 否在从池中取出连接前进行检验
		// config.setTestOnBorrow(true);
		config.setTestOnReturn(true);
		config.setTestWhileIdle(true);
		List<JedisShardInfo> jedisShardInfoList = new ArrayList<JedisShardInfo>();
		JedisShardInfo jedisShardInfo = new JedisShardInfo("hadoop", 12002, 0);
		jedisShardInfo.setSoTimeout(1000 * 10);
		jedisShardInfoList.add(jedisShardInfo);
		JedisShardInfo jedisShardInfo2 = new JedisShardInfo("192.168.16.206",12002,"sla");
		jedisShardInfo.setSoTimeout(1000 * 10);
		jedisShardInfoList.add(jedisShardInfo2);
		// 采用一致性Hash算法分片
		shardPool = new ShardedJedisPool(config,jedisShardInfoList, Hashing.MD5);
	}

	public static ShardedJedis getShardJedis() {
		return shardPool.getResource();
	}

	public static void closeShardJedis(ShardedJedis jedis) {
		shardPool.returnResource(jedis);
	}

}