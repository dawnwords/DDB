package lockmgr;

/**
 * Thrown to indicate that the transaction is deadlocked and should be aborted.
 */
public class DeadlockException extends Exception {
    private long tid;

    public DeadlockException(long tid, String msg) {
        super("The transaction " + tid + " is deadlocked:" + msg);
        this.tid = tid;
    }

    long tid() {
        return tid;
    }
}