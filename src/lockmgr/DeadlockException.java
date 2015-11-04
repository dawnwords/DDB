package lockmgr;

/**
 * Thrown to indicate that the transaction is deadlocked and should be aborted.
 */
public class DeadlockException extends Exception {
    private int tid;

    public DeadlockException(int tid, String msg) {
        super("The transaction " + tid + " is deadlocked:" + msg);
        this.tid = tid;
    }

    int tid() {
        return tid;
    }
}