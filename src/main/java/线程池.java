import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @ClassName 线程池
 * @Description TODO
 * @Author PL
 * @Date 2018/11/27 0:39
 * @Version 1.0
 * @return
 **/

public class 线程池 {
    public static void main(String[] args) {
        //创建一个单个的线程池
       //    ExecutorService test1 = Executors.newSingleThreadExecutor();
        //创建一个固定大小的线程池 ,根据线程优先级
      //     ExecutorService test2 = Executors.newFixedThreadPool(3);
        //创建一个可以缓冲的线程池，这个你想创多少个就多少个，但是也要根据你的实际资源大小
        ExecutorService test3 = Executors.newCachedThreadPool();
        for(int i=0;i<10;i++){
            test3.execute(new Runnable() {
                public void run() {
                    System.out.println("当前线程"+Thread.currentThread().getName());
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        System.out.println("all task is submited");
        test3.shutdownNow();

    }
}
