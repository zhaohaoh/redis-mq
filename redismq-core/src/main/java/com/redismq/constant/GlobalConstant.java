package com.redismq.constant;

public class GlobalConstant {

    /**
     * 分隔符
     */
    public static String SPLITE = ":";

    /**
     * 延时队列拉取的头部消息数量
     */
    public static int DELAY_QUEUE_PULL_SIZE = 100;

    /**
     * 线程数量最大上线
     */
    public static int THREAD_NUM_MAX = 99999;

    /**
     * 延时任务队列阻塞数量
     */
    public static int DELAY_BLOCKING_QUEUE_SIZE = 65536;

    /**
     * 普通队列阻塞数量
     */
    public static int BLOCKING_QUEUE_SIZE = 2048;

    /**
     * 普通队列阻塞数量
     */
    public static int BOSS_NUM = 2;

    /**
     * 客户端过期时间 秒
     */
    public static int CLIENT_EXPIRE = 40;

    /**
     * 客户端循环重平衡时间  秒
     */
    public static int CLIENT_RABALANCE_TIME = 36;

    /**
     * 客户端循环注册时间 秒
     */
    public static int CLIENT_REGISTER_TIME = 30;


    /**
     * 单个虚拟队列消费的锁定时间 有看门狗
     */
    public static int VIRTUAL_LOCK_TIME = 32;
    /**
     * 单个虚拟队列消费看门狗的续期时间
     */
    public static int VIRTUAL_LOCK_WATCH_DOG_TIME = 15;

    /**
     * 工作线程等待停止的时间
     */
    public static int WORK_THREAD_STOP_WAIT = 10;

    /**
     * 打印核心消费日志
     */
    public static boolean PRINT_CONSUME_LOG = true;
}
