package lockmgr;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Dawnwords on 2015/11/1.
 */
public class LockManager {
    public static final long DEADLOCK_TIMEOUT = 10 * 1000;

    private Map<String, LockEntry> keyLockEntryMap;
    private ConcurrentMap<Integer, Queue<String>> tidKeyMap;
    private MonitorThread monitorThread;
    private transient boolean stop;

    public LockManager() {
        this.keyLockEntryMap = Collections.synchronizedMap(new LinkedHashMap<String, LockEntry>());
        this.tidKeyMap = new ConcurrentHashMap<Integer, Queue<String>>();
        this.monitorThread = new MonitorThread();
    }

    public void start() {
        stop = false;
        monitorThread.start();
    }

    public void shutdown() {
        stop = true;
        monitorThread.notify();
    }

    public boolean lock(int tid, String dataKey, LockType lockType) throws DeadlockException {
        if (tid < 0 || dataKey == null) {
            throw new IllegalArgumentException("lock argument error");
        }

        LockEntry lockEntry = keyLockEntryMap.get(dataKey);
        if (lockEntry == null) {
            lockEntry = new LockEntry(dataKey);
        }
        keyLockEntryMap.put(dataKey, lockEntry);
        monitorThread.notify();

        tidKeyMap.putIfAbsent(tid, new ConcurrentLinkedQueue<String>());
        tidKeyMap.get(tid).add(dataKey);

        System.out.println(tidKeyMap);

        lockEntry.addTransaction(tid, lockType);
        return true;
    }

    public boolean unlockAll(int tid) {
        if (tid < 0) {
            throw new IllegalArgumentException("unlock argument error");
        }

        boolean result = true;
        Iterator<String> iterator = tidKeyMap.get(tid).iterator();
        while (iterator.hasNext()) {
            String dataKey = iterator.next();
            LockEntry lockEntry = keyLockEntryMap.get(dataKey);
            if (lockEntry == null) {
                throw new IllegalStateException(String.format("data item with key: %s has not been locked", dataKey));
            }
            if (!lockEntry.release(tid)) {
                result = false;
            }
            if (lockEntry.shouldBeRecycled()) {
                keyLockEntryMap.remove(dataKey);
                iterator.remove();
            }
        }
        return result;
    }

    private class MonitorThread extends Thread {
        @Override
        public void run() {
            while (!stop) {
                Iterator<String> iterator = keyLockEntryMap.keySet().iterator();
                if (iterator.hasNext()) {
                    String key = iterator.next();
                    LockEntry earliestLockEntry = keyLockEntryMap.get(key);
                    if (earliestLockEntry == null) {
                        throw new IllegalStateException("no lock entry for key:" + key);
                    }
                    long deadlockRemaining = earliestLockEntry.deadlockRemaining();
                    if (deadlockRemaining <= 0) {
                        earliestLockEntry.releaseCurrent();
                    } else {
                        try {
                            sleep(deadlockRemaining);
                        } catch (InterruptedException ignored) {
                        }
                    }
                } else {
                    try {
                        wait();
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }
    }
}
