package transaction.exception;


/**
 * The transaction identifier that was passed is not valid. Either the client
 * supplied a bogus Xid, or the transaction has already committed or aborted and
 * cannot be continued.
 */
public class InvalidTransactionException extends RuntimeException {
    public InvalidTransactionException(long Xid, String msg) {
        super("The transaction " + Xid + " is invalid:" + msg);
    }
}