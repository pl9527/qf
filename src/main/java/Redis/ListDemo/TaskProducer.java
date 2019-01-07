package Redis.ListDemo;

import redis.clients.jedis.Jedis;

import java.util.Random;
import java.util.UUID;

/**
 * @ClassName TaskProducer
 * @Description TODO
 * @Author PL
 * @Date 2018/11/30 21:58
 * @Version 1.0
 * @return
 **/

public class TaskProducer {
    private static Jedis jedis = new Jedis("SZ01", 6379);

    public static void main(String[] args) throws InterruptedException {

    }
}
