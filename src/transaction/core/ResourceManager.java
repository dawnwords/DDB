package transaction.core;

import lockmgr.DeadlockException;
import transaction.bean.ResourceItem;
import transaction.exception.InvalidIndexException;

import java.rmi.RemoteException;
import java.util.List;

/**
 * Interface for the Resource Manager of the Distributed Travel Reservation
 * System.
 * <p/>
 * Unlike WorkflowController.java, you are supposed to make changes to this
 * file.
 */

public interface ResourceManager<K> extends Remote {
    /**
     * Query all <code>ResourceItem</code>s related to the transaction with the given xid
     *
     * @param xid transaction id
     * @return all <code>ResourceItem</code>s related to this given xid
     * @throws DeadlockException if a deadlock is detected
     * @throws RemoteException   if one of TM and RMs fails or exceptions occurs in RMI
     */
    List<ResourceItem<K>> query(long xid) throws DeadlockException, RemoteException;

    /**
     * Query all <code>ResourceItem</code>s related to the transaction with the given xid
     * with the given key value
     *
     * @param xid transaction id
     * @param key key of the result ResourceItem to query
     * @return the query result of <code>ResourceItem</code>
     * @throws DeadlockException if a deadlock is detected
     * @throws RemoteException   if one of TM and RMs fails or exceptions occurs in RMI
     */
    ResourceItem<K> query(long xid, K key) throws DeadlockException, RemoteException;

    /**
     * Query all <code>ResourceItem</code>s related to the transaction with the given xid
     * with the given index value
     *
     * @param xid       transaction id
     * @param indexName the indexName to query
     * @param indexVal  the index value to query
     * @return the query result of <code>ResourceItem</code>
     * @throws DeadlockException if a deadlock is detected
     * @throws RemoteException   if one of TM and RMs fails or exceptions occurs in RMI
     */
    List<ResourceItem<K>> query(long xid, String indexName, Object indexVal) throws DeadlockException, RemoteException;

    /**
     * Update the <code>ResourceItem</code> with the given key value related to the transaction of the given xid
     * with the given new value
     *
     * @param xid     transaction id
     * @param key     key of the result ResourceItem to update
     * @param newItem new value
     * @return true if update successfully
     * @throws DeadlockException if a deadlock is detected
     * @throws RemoteException   if one of TM and RMs fails or exceptions occurs in RMI
     */
    boolean update(long xid, K key, ResourceItem<K> newItem) throws DeadlockException, RemoteException;

    /**
     * Insert the given <code>ResourceItem</code> related to the transaction of the given xid
     *
     * @param xid     transaction id
     * @param newItem new <code>ResourceItem</code> to insert
     * @return true if insert successfully
     * @throws DeadlockException if a deadlock is detected
     * @throws RemoteException   if one of TM and RMs fails or exceptions occurs in RMI
     */
    boolean insert(long xid, ResourceItem<K> newItem) throws DeadlockException, RemoteException;


    /**
     * Delete the <code>ResourceItem</code>s related to the transaction with the given xid
     * with the given key value
     *
     * @param xid transaction id
     * @param key key of the result ResourceItem to delete
     * @return true if delete successfully
     * @throws DeadlockException if a deadlock is detected
     * @throws RemoteException   if one of TM and RMs fails or exceptions occurs in RMI
     */
    boolean delete(long xid, K key) throws DeadlockException, RemoteException;


    /**
     * Delete the <code>ResourceItem</code>s related to the transaction with the given xid
     * with the given index value
     *
     * @param xid       transaction id
     * @param indexName the indexName to delete
     * @param indexVal  the value of the index to delete
     * @return true if delete successfully
     * @throws DeadlockException     if a deadlock is detected
     * @throws InvalidIndexException if a deadlock is detected
     * @throws RemoteException       if one of TM and RMs fails or exceptions occurs in RMI
     */
    int delete(long xid, String indexName, Object indexVal) throws DeadlockException, RemoteException;

    /**
     * TM invoke prepare when performing 2-phase commit
     *
     * @param xid transaction id
     * @return if RM get prepared to commit
     * @throws RemoteException if one of TM and RMs fails or exceptions occurs in RMI
     */
    boolean prepare(long xid) throws RemoteException;

    /**
     * TM invoke commit when all RMs related to the transaction with the given xid get prepared
     *
     * @param xid transaction id
     * @throws RemoteException if one of TM and RMs fails or exceptions occurs in RMI
     */
    void commit(long xid) throws RemoteException;

    /**
     * TM invoke abort when some RMs related to the transaction with the given xid are not prepared
     * or Client wants WC to abort this transaction
     *
     * @param xid transaction id
     * @throws RemoteException if one of TM and RMs fails or exceptions occurs in RMI
     */
    void abort(long xid) throws RemoteException;
}