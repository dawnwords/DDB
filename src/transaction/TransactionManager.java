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

    /**
     * The RMI name a TransactionManager binds to.
     */
    String RMIName = "TM";

    boolean dieNow() throws RemoteException;

    void ping() throws RemoteException;

    void enlist(int xid, ResourceManager rm) throws RemoteException;
}
