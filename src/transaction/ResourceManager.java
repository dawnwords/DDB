package transaction;

import lockmgr.DeadlockException;
import transaction.bean.ResourceItem;
import transaction.exception.InvalidIndexException;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Set;

/**
 * Interface for the Resource Manager of the Distributed Travel Reservation
 * System.
 * <p/>
 * Unlike WorkflowController.java, you are supposed to make changes to this
 * file.
 */

public interface ResourceManager<K> extends Remote {

    Set getTransactions() throws RemoteException;

    List<ResourceItem<K>> getUpdatedRows(long xid) throws RemoteException;

    List<ResourceItem<K>> getUpdatedRows() throws RemoteException;

    String getID() throws RemoteException;

    List<ResourceItem<K>> query(long xid) throws DeadlockException, RemoteException;

    ResourceItem<K> query(long xid, K key) throws DeadlockException, RemoteException;

    List<ResourceItem<K>> query(long xid, String indexName, Object indexVal) throws DeadlockException, InvalidIndexException, RemoteException;

    boolean update(long xid, K key, ResourceItem<K> newItem) throws DeadlockException, RemoteException;

    boolean insert(long xid, ResourceItem<K> newItem) throws DeadlockException, RemoteException;

    boolean delete(long xid, K key) throws DeadlockException, RemoteException;

    int delete(long xid, String indexName, Object indexVal) throws DeadlockException, InvalidIndexException, RemoteException;

    boolean prepare(long xid) throws RemoteException;

    void commit(long xid) throws RemoteException;

    void abort(long xid) throws RemoteException;

    void ping() throws RemoteException;

    boolean dieNow() throws RemoteException;

    void setDieTime(DieTime dieTime) throws RemoteException;

    boolean reconnect() throws RemoteException;
}