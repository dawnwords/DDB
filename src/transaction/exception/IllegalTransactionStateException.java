package transaction.exception;

import transaction.TransactionManager;

/**
 * Created by Dawnwords on 2015/12/24.
 */
public class IllegalTransactionStateException extends Exception {
    public IllegalTransactionStateException(long xid, TransactionManager.State state, String message) {
        super(String.format("Illegal State[%s] for xid:%d, %s", state, xid, message));
    }
}
