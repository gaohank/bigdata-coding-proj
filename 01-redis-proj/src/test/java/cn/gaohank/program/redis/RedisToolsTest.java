package cn.gaohank.program.redis;

import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class RedisToolsTest {

    private Jedis jedis;

    @Test
    public void addOne() {
        jedis = new Jedis("192.168.16.100", 12002);
        jedis.ping();
        jedis.quit();
    }

    @Test
    public void testSetString() {
        Jedis jedis = RedisTools.getJedis();
        String set = jedis.set("love", "SUNNING");
        System.out.println(set);
        RedisTools.closeJedis(jedis);
    }

    //
    @Test
    public void testGetString() {
        Jedis jedis = RedisTools.getJedis();
        String string = jedis.get("love");
        System.out.println(string);
        RedisTools.closeJedis(jedis);
    }

    @Test
    public void testString() {
        // -----添加数据----------
        Jedis jedis = RedisTools.getJedis();

        jedis.set("name", "SUNNING");// 向key-->name中放入了value-->sunning
        System.out.println(jedis.get("name"));// 执行结果：sunning

        jedis.append("name", " is my lover"); // 拼接
        System.out.println(jedis.get("name"));

        jedis.del("name"); // 删除某个键
        System.out.println(jedis.get("name"));
        // 设置多个键值对
        jedis.mset("name", "DUODUO", "age", "0", "PH", "138......");
        jedis.incr("age"); // 进行加1操作
        System.out.println(jedis.get("name") + "-" + jedis.get("age") + "-" + jedis.get("PH"));

    }

    /**
     * redis操作Map
     */

    @Test
    public void testMap() {
        // -----添加数据----------
        Jedis jedis = RedisTools.getJedis();

        Map<String, String> map = new HashMap<String, String>();
        map.put("name", "SUNNING");
        map.put("age", "25");
        map.put("PH", "138......");
        jedis.hmset("user", map);

        // 取出user中的name，执行结果:[minxr]-->注意结果是一个泛型的List
        // 第一个参数是存入redis中map对象的key，后面跟的是放入map中的对象的key，后面的key可以跟多个，是可变参数
        List<String> rsmap = jedis.hmget("user", "name", "age", "PH");
        System.out.println(rsmap);

        // 删除map中的某个键值
        jedis.hdel("user", "age");
        System.out.println(jedis.hmget("user", "age")); // 因为删除了，所以返回的是null
        System.out.println(jedis.hlen("user")); // 返回key为user的键中存放的值的个数2
        System.out.println(jedis.exists("user"));// 是否存在key为user的记录 返回true
        System.out.println(jedis.hkeys("user"));// 返回map对象中的所有key
        System.out.println(jedis.hvals("user"));// 返回map对象中的所有value

        Iterator<String> iter = jedis.hkeys("user").iterator();
        while (iter.hasNext()) {
            String key = iter.next();
            System.out.println(key + ":" + jedis.hmget("user", key));
        }
    }
    /**
     * jedis操作List
     */

    @Test
    public void testList(){
        Jedis jedis = RedisTools.getJedis();
        //开始前，先移除所有的内容
        jedis.del("EASTASIA");
        System.out.println(jedis.lrange("EASTASIA",0,-1));
        //先向key java framework中存放三条数据
        jedis.lpush("EASTASIA","CHINA");
        jedis.lpush("EASTASIA","KOREA");
        jedis.lpush("EASTASIA","JAPAN");
        //再取出所有数据jedis.lrange是按范围取出，
        // 第一个是key，第二个是起始位置，第三个是结束位置，jedis.llen获取长度 -1表示取得所有
        System.out.println(jedis.lrange("EASTASIA",0,-1));

        jedis.del("EASTASIA");
        jedis.rpush("EASTASIA","CHINA");
        jedis.rpush("EASTASIA","KOREA");
        jedis.rpush("EASTASIA","JAPAN");
        System.out.println(jedis.lrange("EASTASIA",0,-1));
    }

    /**
     * jedis操作Set
     */
    @Test
    public void testSet(){
        Jedis jedis = RedisTools.getJedis();
        //添加
        jedis.sadd("user","liuling");
        jedis.sadd("user","SUNNING");
        jedis.sadd("user","ling");
        jedis.sadd("user","SUNNING");
        jedis.sadd("user","who");
        //移除noname
        jedis.srem("user","who");
        System.out.println(jedis.smembers("user"));//获取所有加入的value
        System.out.println(jedis.sismember("user", "who"));//判断 who
        //是否是user集合的元素
        System.out.println(jedis.srandmember("user"));
        System.out.println(jedis.scard("user"));//返回集合的元素个数
    }

    @Test
    public void test() throws InterruptedException {
        Jedis jedis = RedisTools.getJedis();
        //jedis 排序
        //注意，此处的rpush和lpush是List的操作。是一个双向链表（但从表现来看的）
        jedis.del("a");//先清除数据，再加入数据进行测试
        jedis.rpush("a", "1");
        jedis.lpush("a","6");
        jedis.lpush("a","3");
        jedis.lpush("a","9");
        System.out.println(jedis.lrange("a",0,-1));// [9, 3, 6, 1]
        System.out.println(jedis.sort("a")); //[1, 3, 6, 9] //输入排序后结果
        System.out.println(jedis.lrange("a",0,-1));
    }

    @Test
    public void testRedisPool() {
        RedisTools.getJedis().set("newname", "中文测试");
        System.out.println(RedisTools.getJedis().get("newname"));
    }
}