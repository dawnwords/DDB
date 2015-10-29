package transaction;

import lockmgr.DeadlockException;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Set;

/**
 * Interface for the Resource Manager of the Distributed Travel Reservation
 * System.
 * <p/>
 * Unlike WorkflowController.java, you are supposed to make changes to this
 * file.
 */

public interface ResourceManager extends Remote {
    /**
     * The RMI names a ResourceManager binds to.
     */
    String RMINameFlights = "RMFlights";
    String RMINameRooms = "RMRooms";
    String RMINameCars = "RMCars";
    String RMINameCustomers = "RMCustomers";

    Set getTransactions() throws RemoteException;

    Collection getUpdatedRows(int xid, String tablename)
            throws RemoteException;

    Collection getUpdatedRows(String tablename) throws RemoteException;

    void setDieTime(String time) throws RemoteException;

    boolean reconnect() throws RemoteException;

    boolean dieNow() throws RemoteException;

    void ping() throws RemoteException;

    String getID() throws RemoteException;

    Collection query(int xid, String tablename)
            throws DeadlockException, InvalidTransactionException,
            RemoteException;

    ResourceItem query(int xid, String tablename, Object key)
            throws DeadlockException, InvalidTransactionException,
            RemoteException;

    Collection query(int xid, String tablename, String indexName,
                     Object indexVal) throws DeadlockException,
            InvalidTransactionException, InvalidIndexException, RemoteException;

    boolean update(int xid, String tablename, Object key,
                   ResourceItem newItem) throws DeadlockException,
            InvalidTransactionException, RemoteException;

    boolean insert(int xid, String tablename, ResourceItem newItem)
            throws DeadlockException, InvalidTransactionException,
            RemoteException;

    boolean delete(int xid, String tablename, Object key)
            throws DeadlockException, InvalidTransactionException,
            RemoteException;

    int delete(int xid, String tablename, String indexName,
               Object indexVal) throws DeadlockException,
            InvalidTransactionException, InvalidIndexException, RemoteException;

    boolean prepare(int xid) throws InvalidTransactionException,
            RemoteException;

    void commit(int xid) throws InvalidTransactionException,
            RemoteException;

    void abort(int xid) throws InvalidTransactionException,
            RemoteException;
}