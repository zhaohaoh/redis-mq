package com.redismq.id;


public class MsgIDGenerator {

    private static volatile DefaultIdWorker defaultIdWorker;

    /**
     * generate UUID using snowflake algorithm
     * @return UUID
     */
    public static long generateId() {
        if (defaultIdWorker == null) {
            synchronized (MsgIDGenerator.class) {
                if (defaultIdWorker == null) {
                    init(1);
                }
            }
        }
        return defaultIdWorker.nextId();
    }
    public static String generateIdStr() {
        if (defaultIdWorker == null) {
            synchronized (MsgIDGenerator.class) {
                if (defaultIdWorker == null) {
                    init(1);
                }
            }
        }
        return String.valueOf(defaultIdWorker.nextId());
    }
    
    public static void init(Integer workId) {
        defaultIdWorker = new DefaultIdWorker(workId);
    }
  
}
