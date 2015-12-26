package transaction;

import lockmgr.DeadlockException;
import lockmgr.LockManager;
import lockmgr.LockType;
import transaction.bean.ResourceItem;
import transaction.exception.*;
import util.IOUtil;
import util.Log;

import java.io.File;
import java.rmi.RemoteException;
import java.util.*;

/**
 * Resource Manager for the Distributed Travel Reservation System.
 * <p/>
 * Description: toy implementation of the RM
 */

public class ResourceManagerImpl<K> extends Host implements ResourceManager<K> {
    private final static String TRANSACTION_LOG_FILENAME = "transactions.log";
    private HashSet<Long> xids;
    private LockManager lm;
    private Hashtable<Long, Hashtable<String, RMTable<K>>> tables;
    private TMDaemon tmDaemon;

    public ResourceManagerImpl(HostName rmiName) throws RemoteException {
        super(rmiName);
        xids = new HashSet<Long>();
        lm = new LockManager();
        tables = new Hashtable<Long, Hashtable<String, RMTable<K>>>();
        tmDaemon = new TMDaemon();
    }

    public void start() {
        recover();
        tmDaemon.start();
        bindRMIRegistry();
    }

    private void recover() {
        HashSet<Long> tXids = loadTransactionLogs();
        if (tXids != null) {
            xids = tXids;
        }

        File dataDir = new File(myRMIName.name());
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }
        File[] dataFiles = dataDir.listFiles();

        if (dataFiles != null) {
            for (File dataFile : dataFiles) {
                if (dataFile.isDirectory()) {
                    recoverXTableDir(dataFile);
                } else if (!dataFile.getName().equals(TRANSACTION_LOG_FILENAME)) {
                    //recover main table
                    getMainTable(dataFile.getName());
                }
            }
        }
    }

    private void recoverXTableDir(File xTableDir) {
        long xid = Long.parseLong(xTableDir.getName());
        if (!xids.contains(xid)) {
            throw new IllegalStateException("RM Recover Error: unexpected xid " + xid);
        }
        File[] xDataFiles = xTableDir.listFiles();
        if (xDataFiles != null) {
            for (File xData : xDataFiles) {
                RMTable xTable = getXTable(xid, xData.getName());
                try {
                    xTable.relockAll();
                } catch (DeadlockException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public Set getTransactions() {
        return xids;
    }

    @Override
    public List<ResourceItem<K>> getUpdatedRows(long xid) {
        RMTable<K> table = getXTable(xid, myRMIName.name());
        return new ArrayList<ResourceItem<K>>(table.table.values());
    }

    @Override
    public List<ResourceItem<K>> getUpdatedRows() {
        RMTable<K> table = getMainTable(myRMIName.name());
        return new ArrayList<ResourceItem<K>>(table.table.values());
    }

    @Override
    public String getID() throws RemoteException {
        return myRMIName.name();
    }

    @Override
    public boolean dieNow() throws RemoteException {
        tmDaemon.interrupt();
        xids.clear();
        tables.clear();
        lm = new LockManager();
        Log.i(myRMIName.name() + " died");
        throw new ResourceManagerUnaccessibleException(myRMIName);
    }

    @Override
    public boolean reconnect() {
        Log.i("%s reconnected", myRMIName.name());
        dieTime = DieTime.NO_DIE;
        recover();
        tmDaemon = new TMDaemon();
        tmDaemon.start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {
        }
        return true;
    }

    public TransactionManager getTransactionManager() throws TransactionManagerUnaccessibleException {
        return tmDaemon.get();
    }

    private RMTable<K> loadTable(long xid, String tableName) {
        return IOUtil.readObject(myRMIName.name() + File.separator + (xid == -1 ? "" : xid + File.separator) + tableName);
    }

    private boolean storeTable(long xid, String tableName, RMTable table) {
        return IOUtil.writeObject(myRMIName.name() + File.separator + xid, tableName, table);
    }

    private RMTable<K> getXTable(long xid, String tableName) {
        Hashtable<String, RMTable<K>> xidTables;
        synchronized (tables) {
            xidTables = tables.get(xid);
            if (xidTables == null) {
                xidTables = new Hashtable<String, RMTable<K>>();
                tables.put(xid, xidTables);
            }
        }
        synchronized (xidTables) {
            RMTable<K> table = xidTables.get(tableName);
            if (table != null) {
                return table;
            }
            table = loadTable(xid, tableName);
            if (table == null) {
                table = new RMTable<K>(tableName, xid == -1 ? null : getMainTable(tableName), xid, lm);
            } else {
                if (xid != -1) {
                    table.setLockManager(lm);
                    table.setParent(getMainTable(tableName));
                }
            }
            xidTables.put(tableName, table);
            return table;
        }
    }

    private RMTable<K> getMainTable(String tableName) {
        return getXTable(-1, tableName);
    }

    private HashSet<Long> loadTransactionLogs() {
        return IOUtil.readObject(myRMIName.name() + File.separator + TRANSACTION_LOG_FILENAME);
    }

    private boolean storeTransactionLogs(HashSet<Long> xids) {
        return IOUtil.writeObject(myRMIName.name(), TRANSACTION_LOG_FILENAME, xids);
    }

    @Override
    public List<ResourceItem<K>> query(long xid) throws DeadlockException, RemoteException {
        try {
            return query(xid, null, null);
        } catch (InvalidIndexException ignored) {
        }
        return null;
    }

    @Override
    public ResourceItem<K> query(long xid, K key) throws DeadlockException, RemoteException {
        addXid(xid);

        RMTable<K> table = getXTable(xid, myRMIName.name());
        ResourceItem<K> item = table.get(key);
        if (item != null && !item.isDeleted()) {
            table.lock(key, LockType.READ);
            if (!storeTable(xid, myRMIName.name(), table)) {
                throw new RemoteException("System Error: Can't write table to disk!");
            }
            return item;
        }
        return null;
    }

    @Override
    public List<ResourceItem<K>> query(long xid, String indexName, Object indexVal) throws DeadlockException, RemoteException {
        addXid(xid);

        List<ResourceItem<K>> result = new ArrayList<ResourceItem<K>>();
        RMTable<K> table = getXTable(xid, myRMIName.name());

        synchronized (table) {
            for (K key : table.keySet()) {
                ResourceItem<K> item = table.get(key);
                if (item != null && !item.isDeleted()) {
                    if (indexName == null || item.getIndex(indexName).equals(indexVal)) {
                        table.lock(key, LockType.READ);
                        result.add(item);
                    }
                }
            }
            if (!result.isEmpty()) {
                if (!storeTable(xid, myRMIName.name(), table)) {
                    throw new RemoteException("System Error: Can't write table to disk!");
                }
            }
        }
        return result;
    }

    @Override
    public boolean update(long xid, K key, ResourceItem<K> newItem) throws DeadlockException, RemoteException {
        if (!key.equals(newItem.getKey()))
            throw new IllegalArgumentException("key:" + key);

        addXid(xid);

        RMTable<K> table = getXTable(xid, myRMIName.name());
        ResourceItem<K> item = table.get(key);
        if (item != null && !item.isDeleted()) {
            table.lock(key, LockType.WRITE);
            table.put(newItem);
            if (!storeTable(xid, myRMIName.name(), table)) {
                throw new RemoteException("System Error: Can't write table to disk!");
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean insert(long xid, ResourceItem<K> newItem) throws DeadlockException, RemoteException {
        addXid(xid);

        RMTable<K> table = getXTable(xid, myRMIName.name());
        ResourceItem<K> item = table.get(newItem.getKey());
        if (item != null && !item.isDeleted()) {
            return false;
        }
        table.lock(newItem.getKey(), LockType.WRITE);
        table.put(newItem);
        if (!storeTable(xid, myRMIName.name(), table)) {
            throw new RemoteException("System Error: Can't write table to disk!");
        }
        return true;
    }

    @Override
    public boolean delete(long xid, K key) throws DeadlockException, RemoteException {
        addXid(xid);

        RMTable<K> table = getXTable(xid, myRMIName.name());
        ResourceItem<K> item = table.get(key);
        if (item != null && !item.isDeleted()) {
            table.lock(key, LockType.WRITE);
            item = item.clone();
            item.delete();
            table.put(item);
            if (!storeTable(xid, myRMIName.name(), table)) {
                throw new RemoteException("System Error: Can't write table to disk!");
            }
            return true;
        }
        return false;
    }

    @Override
    public int delete(long xid, String indexName, Object indexVal) throws DeadlockException, RemoteException {
        addXid(xid);

        int n = 0;
        RMTable<K> table = getXTable(xid, myRMIName.name());
        synchronized (table) {
            for (K key : table.keySet()) {
                ResourceItem<K> item = table.get(key);
                if (item != null && !item.isDeleted() && item.getIndex(indexName).equals(indexVal)) {
                    table.lock(item.getKey(), LockType.WRITE);
                    item = item.clone();
                    item.delete();
                    table.put(item);
                    n++;
                }
            }
            if (n > 0) {
                if (!storeTable(xid, myRMIName.name(), table)) {
                    throw new RemoteException("System Error: Can't write table to disk!");
                }
            }
        }
        return n;
    }

    private void addXid(long xid) throws RemoteException {
        if (xid < 0) {
            throw new InvalidTransactionException(xid, "Xid must be positive.");
        }

        synchronized (xids) {
            xids.add(xid);
            storeTransactionLogs(xids);
        }

        try {
            getTransactionManager().enlist(xid, this);
        } catch (IllegalTransactionStateException e) {
            Log.e(e.getMessage());
        }

        if (dieTime == DieTime.AFTER_ENLIST) {
            dieNow();
        }
    }

    private void checkDie(DieTime dieTime) throws RemoteException {
        if (this.dieTime.alreadyDied(dieTime)) {
            throw new ResourceManagerUnaccessibleException(myRMIName);
        }
        if (this.dieTime == dieTime) {
            dieNow();
        }
    }

    @Override
    public boolean prepare(long xid) throws RemoteException {
        checkDie(DieTime.BEFORE_PREPARE);
        if (xid < 0) {
            throw new InvalidTransactionException(xid, "Xid must be positive.");
        }
        if (!xids.contains(xid)) {
            throw new InvalidTransactionException(xid, "No Such Xid.");
        }
        Log.i("Prepare for %d", xid);
        if (dieTime == DieTime.AFTER_PREPARE) {
            dieNow();
        }
        return true;
    }

    @Override
    public void commit(long xid) throws RemoteException {
        checkDie(DieTime.BEFORE_COMMIT);
        end(xid, true);
    }

    @Override
    public void abort(long xid) throws RemoteException {
        checkDie(DieTime.BEFORE_ABORT);
        end(xid, false);
    }

    private void end(long xid, boolean commit) throws RemoteException {
        if (xid < 0) {
            throw new InvalidTransactionException(xid, "Xid must be positive.");
        }
        Hashtable<String, RMTable<K>> xidTables = tables.get(xid);
        if (xidTables == null) {
            throw new InvalidTransactionException(xid, "No Such Xid.");
        }
        Log.i((commit ? "Commit for " : "Abort for ") + xid);
        synchronized (xidTables) {
            for (String tableName : xidTables.keySet()) {
                if (commit) {
                    commitXTable(xidTables.get(tableName));
                }
                new File(myRMIName.name() + File.separator + xid + File.separator + tableName).delete();
            }
            new File(myRMIName.name() + File.separator + xid).delete();
            tables.remove(xid);
        }
        lm.unlockAll(xid);

        synchronized (xids) {
            xids.remove(xid);
            storeTransactionLogs(xids);
        }
    }

    private void commitXTable(RMTable<K> xTable) throws RemoteException {
        String tableName = xTable.getTableName();
        RMTable<K> table = getMainTable(tableName);
        for (K key : xTable.keySet()) {
            ResourceItem<K> item = xTable.get(key);
            if (item.isDeleted()) {
                table.remove(item);
            } else {
                table.put(item);
            }
        }
        if (!IOUtil.writeObject(myRMIName.name(), tableName, table)) {
            throw new RemoteException("Can't write table to disk");
        }
    }

    private class TMDaemon extends Thread {

        TransactionManager tm;

        @Override
        public void run() {
            while (!isInterrupted()) {
                try {
                    get();
                } catch (TransactionManagerUnaccessibleException ignored) {
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ignored) {
                }
            }
        }

        TransactionManager get() throws TransactionManagerUnaccessibleException {
            try {
                if (tm != null) {
                    tm.ping();
                }
            } catch (Exception e) {
                tm = null;
            }
            if (tm == null) {
                reconnect();
            }
            if (tm == null) {
                Log.e("reconnect tm failed!");
                throw new TransactionManagerUnaccessibleException();
            }
            return tm;
        }

        boolean reconnect() {
            try {
                tm = (TransactionManager) lookUp(HostName.TM);
                Log.i(myRMIName + "'s xids is Empty ? " + xids.isEmpty());
                for (Long xid : xids) {
                    Log.i(myRMIName + " Re-enlist to TM with xid " + xid);
                    tm.enlist(xid, ResourceManagerImpl.this);
                    if (dieTime == DieTime.AFTER_ENLIST) {
                        dieNow();
                    }
                }
                Log.i(myRMIName + " bound to TM");
            } catch (Exception e) {
                Log.e(myRMIName + " enlist error:" + e);
                return false;
            }
            return true;
        }
    }
}