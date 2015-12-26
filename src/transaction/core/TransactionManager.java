package transaction.core;

import transaction.exception.IllegalTransactionStateException;
import transaction.exception.TransactionAbortedException;

import java.rmi.RemoteException;

/**
 * Interface for the Transaction Manager of the Distributed Travel
 * Reservation System.
 * <p/>
 * Unlike WorkflowController.java, you are supposed to make changes
 * to this file.
 */

public interface TransactionManager extends Remote {

    boolean start(long xid) throws RemoteException;

    boolean commit(long xid) throws RemoteException, TransactionAbortedException;

    boolean abort(long xid) throws RemoteException;

    void enlist(long xid, ResourceManager rm) throws RemoteException, IllegalTransactionStateException;

    void ping() throws RemoteException;

    boolean dieNow() throws RemoteException;

    void setDieTime(DieTime dieTime) throws RemoteException;

    boolean reconnect() throws RemoteException;

    enum State {
        Start, Prepare, Commit, Abort, Finish
    }
}
