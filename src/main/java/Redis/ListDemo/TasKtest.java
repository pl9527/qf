package Redis.ListDemo;

import redis.clients.jedis.Jedis;

import java.util.Random;

/**
 * @ClassName TasKtest
 * @Description TODO
 * @Author PL
 * @Date 2018/11/30 21:39
 * @Version 1.0
 * @return
 **/

public class TasKtest {
    private static Jedis jedis=new Jedis("SZ01",6379);
    public static void main(String[] args) throws InterruptedException {
        Random r=new Random();
        while(true){
            Thread.sleep(1000);
                //从任务队列中取出一个任务，放到暂存队列中
            String rpoplpush = jedis.rpoplpush("task1", "task2");
            //模拟处理任务，需要把任务信息从暂存队列中弹出来再放到任务队列里等待继续消费
                if(r.nextInt(19)%9==0){
                   jedis.rpoplpush("task2","task1");
                    System.out.println("任务处理失败"+rpoplpush);
                }
                else{
                    jedis.rpop("task2");
                    System.out.println("任务处理成功");
                }
        }
    }
}
