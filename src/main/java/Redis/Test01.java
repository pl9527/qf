package Redis;

import redis.clients.jedis.Jedis;

import java.util.Map;


/**
 * @ClassName Test01
 * @Description TODO
 * @Author PL
 * @Date 2018/11/30 20:56
 * @Version 1.0
 * @return
 **/

public class Test01 {
    static  Jedis jedis=new Jedis("SZ01",6379);
    public static void main(String[] args) {

        jedis.hset("cart:user001", "T恤", "2");
        jedis.hset("cart:user002", "手机", "5");
        jedis.hset("cart:user002", "电脑", "1");
        getInfo();
   jedis.close();

        }
        public  static void getInfo(){
        jedis.hset("pl","l" ,"T恤");

            String pl = jedis.hget("pl", "1");


            System.out.println(pl);
     jedis.hincrBy("cart:user002","电脑",10000);//自增改变

        }


    }

