package lockmgr;

import util.Log;

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
        monitorThread.wake();
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
        monitorThread.wake();

        tidKeyMap.putIfAbsent(tid, new ConcurrentLinkedQueue<String>());
        tidKeyMap.get(tid).add(dataKey);

        printLockState();

        lockEntry.addTransaction(tid, lockType);
        return true;
    }

    public boolean unlockAll(int tid) {
        if (tid < 0) {
            throw new IllegalArgumentException("unlock argument error");
        }
        Queue<String> keys = tidKeyMap.get(tid);
        if (keys == null) {
            return false;
        }

        boolean result = true;
        Iterator<String> iterator = keys.iterator();
        while (iterator.hasNext()) {
            String dataKey = iterator.next();
            LockEntry lockEntry = keyLockEntryMap.get(dataKey);
            if (lockEntry == null) {
                iterator.remove();
                result = false;
            } else {
                if (!lockEntry.release(tid)) {
                    result = false;
                }
                if (lockEntry.shouldBeRecycled()) {
                    keyLockEntryMap.remove(dataKey);
                    iterator.remove();
                }
            }
        }

        printLockState();
        return result;
    }

    private void printLockState() {
        Log.i("LockState:tid->key:%s,key->lockEntry:%s",
                tidKeyMap,
                keyLockEntryMap);
    }

    private class MonitorThread extends Thread {

        private final Object mutex = new Object();

        public MonitorThread() {
            super("Deadlock Monitor");
        }

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
                        int tid = earliestLockEntry.releaseCurrent();
                        if (earliestLockEntry.shouldBeRecycled()) {
                            keyLockEntryMap.remove(key);
                            Queue<String> keys = tidKeyMap.get(tid);
                            if (keys != null) {
                                keys.remove(key);
                            }
                        }
                    } else {
                        try {
                            sleep(deadlockRemaining);
                        } catch (InterruptedException ignored) {
                        }
                    }
                } else {
                    pend();
                }
            }
        }

        void pend() {
            synchronized (mutex) {
                try {
                    mutex.wait();
                } catch (InterruptedException ignored) {
                }
            }
        }

        void wake() {
            synchronized (mutex) {
                mutex.notify();
            }
        }
    }
}
