package transaction.exception;

/**
 * A problem occurred that caused the transaction to abort.  Perhaps
 * deadlock was the problem, or perhaps a device or communication
 * failure caused this operation to abort the transaction.
 */
public class TransactionAbortedException extends Exception {
    public TransactionAbortedException(long xid, String msg) {
        super("The transaction " + xid + " aborted:" + msg);
    }
}
