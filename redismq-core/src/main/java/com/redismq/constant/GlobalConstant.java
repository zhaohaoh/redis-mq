package com.redismq.constant;

public class GlobalConstant {

    /**
     * 分隔符
     */
    public static String SPLITE = ":";

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
     * boss线程
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
     * 工作线程等待停止的时间
     */
    public static int WORK_THREAD_STOP_WAIT = 10;

}