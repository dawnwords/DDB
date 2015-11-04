package lockmgr;

import java.util.Date;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by Dawnwords on 2015/11/1.
 */
public class LockEntry {

    private String dataKey;
    private Queue<TransactionStatus> waitingQueue;

    public LockEntry(String dataKey) {
        this.dataKey = dataKey;
        this.waitingQueue = new ConcurrentLinkedQueue<TransactionStatus>();
    }

    public TransactionStatus currentStatus() {
        if (waitingQueue.isEmpty()) {
            throw new IllegalStateException("No one holds lock for " + dataKey);
        }
        return waitingQueue.peek();
    }

    public long deadlockRemaining() {
        return currentStatus().lockTime().getTime() + LockManager.DEADLOCK_TIMEOUT - System.currentTimeMillis();
    }

    public void addTransaction(int tid, LockType lockType) throws DeadlockException {
        TransactionStatus status = getTransactionStatus(tid);
        if (status == null) {
            status = new TransactionStatus(tid, new Date(), lockType);
            waitingQueue.add(status);
            if (shouldPending(status, lockType)) {
                status.pending();
            }
        } else {
            status.lockUpgrade();
        }
    }

    private boolean shouldPending(TransactionStatus status, LockType lockType) {
        TransactionStatus currentStatus = waitingQueue.peek();
        return currentStatus != status && !currentStatus.lockType().isShared(lockType);
    }

    public void releaseCurrent() {
        waitingQueue.poll();
        passLock();
    }

    private void passLock() {
        TransactionStatus currentStatus = waitingQueue.peek();
        if (currentStatus != null) {
            switch (currentStatus.lockType()) {
                case READ:
                    for (TransactionStatus status : waitingQueue) {
                        if (status.lockType() == LockType.READ) {
                            status.activate();
                        }
                    }
                    break;
                case WRITE:
                    currentStatus.activate();
                    break;
            }
        }
    }

    public boolean release(int tid) {
        if (currentStatus().tid() == tid) {
            releaseCurrent();
            return true;
        }
        TransactionStatus status = getTransactionStatus(tid);
        if (status == null) {
            // no such tid or lock has been occupied
            return false;
        }

        if (status.lockType() == LockType.READ) {
            waitingQueue.remove();
            return true;
        }
        throw new IllegalStateException("no way to release a WRITE lock waiting");
    }

    public boolean shouldBeRecycled() {
        return waitingQueue.isEmpty();
    }

    private TransactionStatus getTransactionStatus(int tid) {
        for (TransactionStatus status : waitingQueue) {
            if (status.tid() == tid) {
                return status;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "LockEntry{" +
                "dataKey='" + dataKey + '\'' +
                ", waitingQueue=" + waitingQueue +
                '}';
    }
}
