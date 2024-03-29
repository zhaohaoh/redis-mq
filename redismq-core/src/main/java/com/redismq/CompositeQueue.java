package com.redismq;

import java.util.*;
import java.util.concurrent.*;

/**
 * @Author: hzh
 * @Date: 2022/8/30 14:21
 * 组合队列-组合模式
 */
public class CompositeQueue<E> extends AbstractQueue<E> implements BlockingQueue<E>, java.io.Serializable {
    private final List<BlockingQueue<E>> blockingQueues = new ArrayList<>();
    private static final long serialVersionUID = -6903933977591709194L;

    //队列的数量，自定义实现队列，只有队列的size满的时候才开启更多线程消费,消费者线程数满的时候。利用了信号量进行阻塞处理。
    //多余了，线程池默认的就是这样。这里本来是想先创建非核心线程，然后再加入队列，那么这里换位置是可以实现的
    public CompositeQueue(int size) {
        blockingQueues.add(new LinkedBlockingQueue<>(size));
        blockingQueues.add(new SynchronousQueue<>());
    }


    @Override
    public Iterator iterator() {
        return blockingQueues.get(0).iterator();
    }

    @Override
    public int size() {
        return blockingQueues.get(0).size();
    }

    @Override
    public void put(E o) throws InterruptedException {
        for (BlockingQueue<E> blockingQueue : blockingQueues) {
            blockingQueue.put(o);
        }
    }

    @Override
    public boolean offer(E o, long timeout, TimeUnit unit) throws InterruptedException {
        for (BlockingQueue<E> blockingQueue : blockingQueues) {
            boolean offer = blockingQueue.offer(o, timeout, unit);
            if (offer) {
                return offer;
            }
        }
        return false;
    }

    @Override
    public E take() throws InterruptedException {
        // 线程数没有超过核心线程数就只take第一个队列
        for (BlockingQueue<E> blockingQueue : blockingQueues) {
            return blockingQueue.take();
        }
        return null;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        for (int i = 0; i < blockingQueues.size(); i++) {
            if (i < blockingQueues.size() - 1) {
                E poll = blockingQueues.get(i).poll();
                if (poll != null) {
                    return poll;
                }
            } else {
                return blockingQueues.get(i).poll(timeout, unit);
            }
        }
        return null;
    }

    @Override
    public int remainingCapacity() {
        return blockingQueues.get(0).remainingCapacity();
    }

    @Override
    public int drainTo(Collection c) {
        return blockingQueues.get(0).drainTo(c);
    }

    @Override
    public int drainTo(Collection c, int maxElements) {
        return blockingQueues.get(0).drainTo(c, maxElements);
    }

    @Override
    public boolean offer(E o) {
        for (BlockingQueue<E> blockingQueue : blockingQueues) {
            boolean offer = blockingQueue.offer(o);
            if (offer) {
                return offer;
            }
        }
        return false;
    }

    @Override
    public E poll() {
        for (BlockingQueue<E> blockingQueue : blockingQueues) {
            E poll = blockingQueue.poll();
            if (poll != null) {
                return poll;
            }
        }
        return null;
    }

    @Override
    public E peek() {
        for (BlockingQueue<E> blockingQueue : blockingQueues) {
            E peek = blockingQueue.peek();
            if (peek != null) {
                return peek;
            }
        }
        return null;
    }

    public static void main(String[] args) {
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(), new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                System.out.println("被拒绝");
            }
        });
        for (int i = 0; i < 100; i++) {
            threadPoolExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }
}
