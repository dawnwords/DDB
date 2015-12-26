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

    /**
     * WC invokes this method to start a transaction with the given xid
     * TM initializes all the states related to a transaction
     *
     * @param xid transaction id
     * @return true if the xid is legal and start successfully
     * @throws RemoteException if exceptions occurs in RMI
     */
    boolean start(long xid) throws RemoteException;

    /**
     * WC invokes this method when Client wants to commit the transaction with the given xid
     *
     * @param xid transaction id
     * @return true if the traction commit successfully
     * @throws RemoteException             if one of RMs fails or exceptions occurs in RMI
     * @throws TransactionAbortedException if there is any reason to abort the transaction
     */
    boolean commit(long xid) throws RemoteException, TransactionAbortedException;


    /**
     * WC invokes this method when Client wants to abort the transaction with the given xid
     *
     * @param xid transaction id
     * @return true if the traction abort successfully
     * @throws RemoteException if one of RMs fails or exceptions occurs in RMI
     */
    boolean abort(long xid) throws RemoteException;

    /**
     * RM invokes this method to tell TM that it is involved in the transaction with the given xid
     * and parse the reference to TM
     *
     * @param xid transaction id
     * @param rm  RM reference
     * @throws RemoteException                  if exceptions occurs in RMI
     * @throws IllegalTransactionStateException if the state of the given transaction is not correct
     */
    void enlist(long xid, ResourceManager rm) throws RemoteException, IllegalTransactionStateException;

    /**
     * Transaction States Enumerator
     */
    enum State {
        Start, Prepare, Commit, Abort, Finish
    }
}
