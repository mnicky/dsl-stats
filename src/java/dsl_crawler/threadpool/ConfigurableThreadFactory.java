package dsl_crawler.threadpool;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ConfigurableThreadFactory implements ThreadFactory {
    private static final AtomicInteger poolNumber = new AtomicInteger(1);
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;
    private final boolean daemonThreads;

    public ConfigurableThreadFactory(String namePrefix, boolean daemonThreads) {
        SecurityManager s = System.getSecurityManager();
        this.group = (s != null)? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        this.namePrefix = (namePrefix == null ? "ConfigurableThreadFactory" : namePrefix) +
                           "-pool-" + poolNumber.getAndIncrement() + "-thread-";
        this.daemonThreads = daemonThreads;
    }

    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
        t.setDaemon(daemonThreads);
        if (t.getPriority() != Thread.NORM_PRIORITY)
            t.setPriority(Thread.NORM_PRIORITY);
        return t;
    }
}
