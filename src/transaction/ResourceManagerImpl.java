package transaction;

import lockmgr.DeadlockException;
import lockmgr.LockManager;
import lockmgr.LockType;
import transaction.bean.ResourceItem;
import transaction.exception.InvalidIndexException;
import transaction.exception.InvalidTransactionException;
import transaction.exception.TransactionManagerUnaccessibleException;
import util.IOUtil;

import java.io.File;
import java.io.FileInputStream;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;

/**
 * Resource Manager for the Distributed Travel Reservation System.
 * <p/>
 * Description: toy implementation of the RM
 */

public class ResourceManagerImpl<K> extends Host implements ResourceManager<K> {
    private final static String TRANSACTION_LOG_FILENAME = "transactions.log";
    private HashSet<Integer> xids;
    private LockManager lm;
    private Hashtable<Integer, Hashtable<String, RMTable<K>>> tables;
    private TMDaemon tmDaemon;

    public ResourceManagerImpl(HostName rmiName) throws RemoteException {
        super(rmiName);
        xids = new HashSet<Integer>();
        lm = new LockManager();
        tables = new Hashtable<Integer, Hashtable<String, RMTable<K>>>();
        tmDaemon = new TMDaemon();
    }

    public void start() {
        if (myRMIName == null) {
            throw new IllegalStateException("No RMI name given");
        }
        recover();
        tmDaemon.start();
        bindRMIRegistry();
    }

    public void recover() {
        HashSet<Integer> tXids = loadTransactionLogs();
        if (tXids != null) {
            xids = tXids;
        }

        File dataDir = new File("data");
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }
        File[] dataFiles = dataDir.listFiles();

        if (dataFiles != null) {
            for (File dataFile : dataFiles) {
                if (dataFile.isDirectory()) {
                    //xTable
                    int xid = Integer.parseInt(dataFile.getName());
                    if (!xids.contains(xid)) {
                        throw new IllegalStateException("RM Recover Error: unexpected xid " + xid);
                    }
                    File[] xDataFiles = dataFile.listFiles();
                    if (xDataFiles != null) {
                        for (File xData : xDataFiles) {
                            RMTable xTable = getTable(xid, xData.getName());
                            try {
                                xTable.relockAll();
                            } catch (DeadlockException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                } else {
                    //main table
                    if (dataFile.getName().equals(TRANSACTION_LOG_FILENAME)) {
                        getTable(dataFile.getName());
                    }
                }
            }
        }
    }

    private void bindRMIRegistry() {
        String rmiPort = getProperty("rm." + myRMIName + ".port");
        try {
            Registry rmiRegistry = LocateRegistry.createRegistry(Integer.parseInt(rmiPort));
            rmiRegistry.bind(myRMIName.name(), this);
            System.out.println(myRMIName + " bound");
        } catch (Exception e) {
            throw new RuntimeException(myRMIName + " not bound:" + e);
        }
    }

    @Override
    public Set getTransactions() {
        return xids;
    }

    @Override
    public List<ResourceItem<K>> getUpdatedRows(int xid) {
        RMTable<K> table = getTable(xid, myRMIName.name());
        return new ArrayList<ResourceItem<K>>(table.table.values());
    }

    @Override
    public List<ResourceItem<K>> getUpdatedRows() {
        RMTable<K> table = getTable(myRMIName.name());
        return new ArrayList<ResourceItem<K>>(table.table.values());
    }

    @Override
    public String getID() throws RemoteException {
        return myRMIName.name();
    }

    @Override
    public boolean reconnect() {
        return tmDaemon.reconnect();
    }

    public TransactionManager getTransactionManager() throws TransactionManagerUnaccessibleException {
        TransactionManager tm = tmDaemon.get();
        if (tm == null)
            throw new TransactionManagerUnaccessibleException();
        else
            return tm;
    }

    private RMTable<K> loadTable(int xid, String tableName) {
        return IOUtil.readObject("data" + File.separator + (xid == -1 ? "" : xid + File.separator) + tableName);
    }

    private boolean storeTable(int xid, String tableName, RMTable table) {
        return IOUtil.writeObject("data" + File.separator + xid, tableName, table);
    }

    private RMTable<K> getTable(int xid, String tableName) {
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
            if (table != null)
                return table;
            table = loadTable(xid, tableName);
            if (table == null) {
                if (xid == -1)
                    table = new RMTable<K>(tableName, null, -1, lm);
                else {
                    table = new RMTable<K>(tableName, getTable(tableName), xid, lm);
                }
            } else {
                if (xid != -1) {
                    table.setLockManager(lm);
                    table.setParent(getTable(tableName));
                }
            }
            xidTables.put(tableName, table);
            return table;
        }
    }

    private RMTable<K> getTable(String tableName) {
        return getTable(-1, tableName);
    }

    private HashSet<Integer> loadTransactionLogs() {
        return IOUtil.readObject("data" + File.separator + "transactions.log");
    }

    private boolean storeTransactionLogs(HashSet<Integer> xids) {
        return IOUtil.writeObject("data", "transactions.log", xids);
    }

    @Override
    public List<ResourceItem<K>> query(int xid) throws DeadlockException, InvalidTransactionException, RemoteException {
        try {
            return query(xid, null, null);
        } catch (InvalidIndexException ignored) {
        }
        return null;
    }

    @Override
    public ResourceItem<K> query(int xid, K key) throws DeadlockException, InvalidTransactionException, RemoteException {
        addXid(xid);

        RMTable<K> table = getTable(xid, myRMIName.name());
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
    public List<ResourceItem<K>> query(int xid, String indexName, Object indexVal) throws DeadlockException, InvalidTransactionException, InvalidIndexException, RemoteException {
        addXid(xid);

        List<ResourceItem<K>> result = new ArrayList<ResourceItem<K>>();
        RMTable<K> table = getTable(xid, myRMIName.name());

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
    public boolean update(int xid, K key, ResourceItem<K> newItem) throws DeadlockException, InvalidTransactionException, RemoteException {
        if (!key.equals(newItem.getKey()))
            throw new IllegalArgumentException();

        addXid(xid);

        RMTable<K> table = getTable(xid, myRMIName.name());
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
    public boolean insert(int xid, ResourceItem<K> newItem) throws DeadlockException, InvalidTransactionException, RemoteException {
        addXid(xid);

        RMTable<K> table = getTable(xid, myRMIName.name());
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
    public boolean delete(int xid, K key) throws DeadlockException, InvalidTransactionException, RemoteException {
        addXid(xid);

        RMTable<K> table = getTable(xid, myRMIName.name());
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
    public int delete(int xid, String indexName, Object indexVal) throws DeadlockException, InvalidTransactionException, RemoteException {
        addXid(xid);

        int n = 0;
        RMTable<K> table = getTable(xid, myRMIName.name());
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

    private void addXid(int xid) throws RemoteException, InvalidTransactionException {
        if (xid < 0) {
            throw new InvalidTransactionException(xid, "Xid must be positive.");
        }

        try {
            synchronized (xids) {
                xids.add(xid);
                storeTransactionLogs(xids);
            }
            getTransactionManager().enlist(xid, this);
        } catch (TransactionManagerUnaccessibleException e) {
            throw new RemoteException(e.getLocalizedMessage(), e);
        }

        if (dieTime == DieTime.AFTER_ENLIST) {
            dieNow();
        }
    }

    @Override
    public boolean prepare(int xid) throws InvalidTransactionException, RemoteException {
        if (dieTime == DieTime.BEFORE_PREPARE) {
            dieNow();
        }
        if (xid < 0) {
            throw new InvalidTransactionException(xid, "Xid must be positive.");
        }
        if (dieTime == DieTime.AFTER_PREPARE)
            dieNow();
        return true;
    }

    @Override
    public void commit(int xid) throws InvalidTransactionException, RemoteException {
        if (dieTime == DieTime.BEFORE_COMMIT) {
            dieNow();
        }
        end(xid, true);
    }

    @Override
    public void abort(int xid) throws InvalidTransactionException, RemoteException {
        if (dieTime == DieTime.BEFORE_ABORT) {
            dieNow();
        }
        end(xid, false);
    }

    private void end(int xid, boolean commit) throws InvalidTransactionException, RemoteException {
        if (xid < 0) {
            throw new InvalidTransactionException(xid, "Xid must be positive.");
        }
        Hashtable<String, RMTable<K>> xidTables = tables.get(xid);
        if (xidTables != null) {
            synchronized (xidTables) {
                for (String tableName : xidTables.keySet()) {
                    if (commit) {
                        commitXTable(xidTables.get(tableName));
                    }
                    new File("data/" + xid + "/" + tableName).delete();
                }
                new File("data/" + xid).delete();
                tables.remove(xid);
            }
        }
        if (!lm.unlockAll(xid)) {
            throw new RuntimeException();
        }
        synchronized (xids) {
            xids.remove(xid);
        }
    }

    private void commitXTable(RMTable<K> xTable) throws RemoteException {
        String tableName = xTable.getTableName();
        RMTable<K> table = getTable(tableName);
        for (K key : xTable.keySet()) {
            ResourceItem<K> item = xTable.get(key);
            if (item.isDeleted()) {
                table.remove(item);
            } else {
                table.put(item);
            }
        }
        if (!IOUtil.writeObject("data", tableName, table)) {
            throw new RemoteException("Can't write table to disk");
        }
    }

    private String getProperty(String key) {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream("conf/ddb.conf"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return prop.getProperty(key);
    }

    private class TMDaemon extends Thread {

        TransactionManager tm;

        @Override
        public void run() {
            while (!isInterrupted()) {
                try {
                    if (tm != null) {
                        tm.ping();
                    }
                } catch (Exception e) {
                    tm = null;
                }

                if (tm == null) {
                    reconnect();
                    System.out.println("reconnect tm!");
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ignored) {
                }
            }
        }

        TransactionManager get() {
            return tm;
        }

        boolean reconnect() {
            String rmiPort = getProperty("tm.port");
            if (rmiPort == null) {
                rmiPort = "";
            } else if (!rmiPort.equals("")) {
                rmiPort = "//:" + rmiPort + "/";
            }

            try {
                tm = (TransactionManager) Naming.lookup(rmiPort + HostName.TM);
                System.out.println(myRMIName + "'s xids is Empty ? " + xids.isEmpty());
                for (Integer xid : xids) {
                    System.out.println(myRMIName + " Re-enlist to TM with xid" + xid);
                    tm.enlist(xid, ResourceManagerImpl.this);
                    if (dieTime == DieTime.AFTER_ENLIST) {
                        dieNow();
                    }
                }
                System.out.println(myRMIName + " bound to TM");
            } catch (Exception e) {
                System.err.println(myRMIName + " enlist error:" + e);
                return false;
            }
            return true;
        }
    }
}