package lockmgr;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Dawnwords on 2015/11/1.
 */
public class TransactionStatus implements Comparable<TransactionStatus> {

    private static final SimpleDateFormat FORMAT = new SimpleDateFormat("%tH:%tM:%tS %L");
    private int tid;
    private Date lockTime;
    private LockType lockType;
    private Thread waitingThread;

    public TransactionStatus(int tid, Date lockTime, LockType lockType) {
        this.tid = tid;
        this.lockTime = lockTime;
        this.lockType = lockType;
        this.waitingThread = Thread.currentThread();
    }

    public int tid() {
        return tid;
    }

    public Date lockTime() {
        return lockTime;
    }

    public LockType lockType() {
        return lockType;
    }

    public void pending() throws DeadlockException {
        try {
            waitingThread.wait(LockManager.DEADLOCK_TIMEOUT);
            throw new DeadlockException(tid, "Sleep timeout...deadlock.");
        } catch (InterruptedException ignored) {
        }
    }

    public void activate() {
        waitingThread.notify();
    }

    @Override
    public int compareTo(TransactionStatus o) {
        return lockTime.compareTo(o.lockTime);
    }

    public boolean lockUpgrade() {
        if (lockType == LockType.READ) {
            lockType = LockType.WRITE;
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("{%d%s:%s@%s}",
                tid,
                waitingThread.getState() == Thread.State.WAITING ? "[P]" : "",
                lockType,
                FORMAT.format(lockTime));
    }
}
