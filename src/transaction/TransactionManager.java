package transaction;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Interface for the Transaction Manager of the Distributed Travel
 * Reservation System.
 * <p/>
 * Unlike WorkflowController.java, you are supposed to make changes
 * to this file.
 */

public interface TransactionManager extends Remote {

    boolean start(int xid) throws RemoteException;

    boolean commit(int xid) throws RemoteException;

    boolean abort(int xid) throws RemoteException;

    void enlist(int xid, ResourceManager rm) throws RemoteException;

    void ping() throws RemoteException;

    boolean dieNow() throws RemoteException;

    void setDieTime(DieTime dieTime) throws RemoteException;

    boolean reconnect() throws RemoteException;
}
