package transaction;

import lockmgr.DeadlockException;
import lockmgr.LockManager;
import lockmgr.LockType;
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
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

/**
 * Resource Manager for the Distributed Travel Reservation System.
 * <p/>
 * Description: toy implementation of the RM
 */

public class ResourceManagerImpl extends UnicastRemoteObject implements ResourceManager {
    protected final static String TRANSACTION_LOG_FILENAME = "transactions.log";
    protected String myRMIName = null; // Used to distinguish this RM from other
    protected DieTime dieTime;
    // RMs
    protected HashSet xids = new HashSet();
    protected TransactionManager tm = null;
    protected LockManager lm = new LockManager();
    protected Hashtable tables = new Hashtable();

    private volatile boolean stop;

    public ResourceManagerImpl(String rmiName) throws RemoteException {
        myRMIName = rmiName;
        dieTime = DieTime.NO_DIE;
    }

    //  test usage
    public ResourceManagerImpl() throws RemoteException {
    }

    public void start() {
        recover();

        new Thread() {
            public void run() {
                while (!stop) {
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
                    sleepForAWhile();
                }
            }
        }.start();


        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream("conf/ddb.conf"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        String rmiPort = prop.getProperty("rm." + myRMIName + ".port");
        Registry _rmiRegistry;
        try {
            _rmiRegistry = LocateRegistry.createRegistry(Integer.parseInt(rmiPort));
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }

        if (myRMIName.equals("")) {
            throw new IllegalStateException("No RMI name given");
        }

        try {
            _rmiRegistry.bind(myRMIName, this);
            System.out.println(myRMIName + " bound");
        } catch (Exception e) {
            throw new RuntimeException(myRMIName + " not bound:" + e);
        }
    }

    private void sleepForAWhile() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {
        }
    }

    public Set getTransactions() {
        return xids;
    }

    public Collection getUpdatedRows(int xid, String tableName) {
        RMTable table = getTable(xid, tableName);
        return new ArrayList(table.table.values());
    }

    public Collection getUpdatedRows(String tableName) {
        RMTable table = getTable(tableName);
        return new ArrayList(table.table.values());
    }

    public void setDieTime(DieTime dieTime) throws RemoteException {
        this.dieTime = dieTime;
        System.out.println("Die time set to : " + dieTime);
    }

    public String getID() throws RemoteException {
        return myRMIName;
    }

    public void ping() {
    }

    public void recover() {
        HashSet tXids = loadTransactionLogs();
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
                    //main table
                    if (dataFile.getName().equals(TRANSACTION_LOG_FILENAME)) {
                        getTable(dataFile.getName());
                    }
                } else {
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
                }
            }
        }
    }

    public boolean reconnect() {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream("conf/ddb.conf"));
        } catch (Exception e1) {
            e1.printStackTrace();
            return false;
        }
        String rmiPort = prop.getProperty("tm.port");
        if (rmiPort == null) {
            rmiPort = "";
        } else if (!rmiPort.equals("")) {
            rmiPort = "//:" + rmiPort + "/";
        }

        try {
            tm = (TransactionManager) Naming.lookup(rmiPort + TransactionManager.RMIName);
            System.out.println(myRMIName + "'s xids is Empty ? " + xids.isEmpty());
            for (Object xid1 : xids) {
                int xid = (Integer) xid1;
                System.out.println(myRMIName + " Re-enlist to TM with xid" + xid);
                tm.enlist(xid, this);
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

    public boolean dieNow() throws RemoteException {
        stop = true;
        sleepForAWhile();
        System.exit(1);
        return true;
    }

    public TransactionManager getTransactionManager() throws TransactionManagerUnaccessibleException {
        if (tm != null) {
            try {
                tm.ping();
            } catch (RemoteException e) {
                tm = null;
            }
        }
        if (tm == null) {
            if (!reconnect())
                tm = null;
        }
        if (tm == null)
            throw new TransactionManagerUnaccessibleException();
        else
            return tm;
    }

    public void setTransactionManager(TransactionManager tm) {
        this.tm = tm;
    }

    protected LockManager getLockManager() {
        return lm;
    }

    protected RMTable loadTable(int xid, String tableName) {
        return IOUtil.readObject("data" + File.separator + (xid == -1 ? "" : xid + File.separator) + tableName);
    }

    protected boolean storeTable(int xid, String tableName, RMTable table) {
        return IOUtil.writeObject("data" + File.separator + xid, tableName, table);
    }

    protected RMTable getTable(int xid, String tableName) {
        Hashtable xidTables;
        synchronized (tables) {
            xidTables = (Hashtable) tables.get(xid);
            if (xidTables == null) {
                xidTables = new Hashtable();
                tables.put(xid, xidTables);
            }
        }
        synchronized (xidTables) {
            RMTable table = (RMTable) xidTables.get(tableName);
            if (table != null)
                return table;
            table = loadTable(xid, tableName);
            if (table == null) {
                if (xid == -1)
                    table = new RMTable(tableName, null, -1, lm);
                else {
                    table = new RMTable(tableName, getTable(tableName), xid, lm);
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

    protected RMTable getTable(String tableName) {
        return getTable(-1, tableName);
    }

    protected HashSet loadTransactionLogs() {
        return IOUtil.readObject("data" + File.separator + "transactions.log");
    }

    protected boolean storeTransactionLogs(HashSet xids) {
        return IOUtil.writeObject("data", "transactions.log", xids);
    }

    public Collection query(int xid, String tableName) throws DeadlockException, InvalidTransactionException, RemoteException {
        addXid(xid);

        Collection result = new ArrayList();
        RMTable table = getTable(xid, tableName);
        synchronized (table) {
            for (Object key : table.keySet()) {
                ResourceItem item = table.get(key);
                if (item != null && !item.isDeleted()) {
                    table.lock(key, LockType.READ);
                    result.add(item);
                }
            }
            if (!result.isEmpty()) {
                if (!storeTable(xid, tableName, table)) {
                    throw new RemoteException("System Error: Can't write table to disk!");
                }
            }
        }
        return result;
    }

    public ResourceItem query(int xid, String tableName, Object key) throws DeadlockException, InvalidTransactionException, RemoteException {
        addXid(xid);

        RMTable table = getTable(xid, tableName);
        ResourceItem item = table.get(key);
        if (item != null && !item.isDeleted()) {
            table.lock(key, LockType.READ);
            if (!storeTable(xid, tableName, table)) {
                throw new RemoteException("System Error: Can't write table to disk!");
            }
            return item;
        }
        return null;
    }

    public Collection query(int xid, String tableName, String indexName, Object indexVal) throws DeadlockException, InvalidTransactionException, InvalidIndexException, RemoteException {
        addXid(xid);

        Collection result = new ArrayList();
        RMTable table = getTable(xid, tableName);
        synchronized (table) {
            for (Object key : table.keySet()) {
                ResourceItem item = table.get(key);
                if (item != null && !item.isDeleted() && item.getIndex(indexName).equals(indexVal)) {
                    table.lock(key, LockType.READ);
                    result.add(item);
                }
            }
            if (!result.isEmpty()) {
                if (!storeTable(xid, tableName, table)) {
                    throw new RemoteException("System Error: Can't write table to disk!");
                }
            }
        }
        return result;
    }

    public boolean update(int xid, String tableName, Object key, ResourceItem newItem) throws DeadlockException, InvalidTransactionException, RemoteException {
        if (!key.equals(newItem.getKey()))
            throw new IllegalArgumentException();

        addXid(xid);

        RMTable table = getTable(xid, tableName);
        ResourceItem item = table.get(key);
        if (item != null && !item.isDeleted()) {
            table.lock(key, LockType.WRITE);
            table.put(newItem);
            if (!storeTable(xid, tableName, table)) {
                throw new RemoteException("System Error: Can't write table to disk!");
            }
            return true;
        }
        return false;
    }

    public boolean insert(int xid, String tableName, ResourceItem newItem) throws DeadlockException, InvalidTransactionException, RemoteException {
        addXid(xid);

        RMTable table = getTable(xid, tableName);
        ResourceItem item = table.get(newItem.getKey());
        if (item != null && !item.isDeleted()) {
            return false;
        }
        table.lock(newItem.getKey(), LockType.WRITE);
        table.put(newItem);
        if (!storeTable(xid, tableName, table)) {
            throw new RemoteException("System Error: Can't write table to disk!");
        }
        return true;
    }

    public boolean delete(int xid, String tableName, Object key) throws DeadlockException, InvalidTransactionException, RemoteException {
        addXid(xid);

        RMTable table = getTable(xid, tableName);
        ResourceItem item = table.get(key);
        if (item != null && !item.isDeleted()) {
            table.lock(key, LockType.WRITE);
            item = (ResourceItem) item.clone();
            item.delete();
            table.put(item);
            if (!storeTable(xid, tableName, table)) {
                throw new RemoteException("System Error: Can't write table to disk!");
            }
            return true;
        }
        return false;
    }

    public int delete(int xid, String tableName, String indexName, Object indexVal) throws DeadlockException, InvalidTransactionException, InvalidIndexException, RemoteException {
        if (xid < 0) {
            throw new InvalidTransactionException(xid, "Xid must be positive.");
        }
        addXid(xid);

        int n = 0;
        RMTable table = getTable(xid, tableName);
        synchronized (table) {
            for (Object key : table.keySet()) {
                ResourceItem item = table.get(key);
                if (item != null && !item.isDeleted() && item.getIndex(indexName).equals(indexVal)) {
                    table.lock(item.getKey(), LockType.WRITE);
                    item = (ResourceItem) item.clone();
                    item.delete();
                    table.put(item);
                    n++;
                }
            }
            if (n > 0) {
                if (!storeTable(xid, tableName, table)) {
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

    public boolean prepare(int xid) throws InvalidTransactionException, RemoteException {
        if (dieTime == DieTime.BEFORE_PREPARE)
            dieNow();
        if (xid < 0) {
            throw new InvalidTransactionException(xid, "Xid must be positive.");
        }
        if (dieTime == DieTime.AFTER_PREPARE)
            dieNow();
        return true;
    }

    public void commit(int xid) throws InvalidTransactionException, RemoteException {
        if (dieTime == DieTime.BEFORE_COMMIT)
            dieNow();
        if (xid < 0) {
            throw new InvalidTransactionException(xid, "Xid must be positive.");
        }
        Hashtable xidTables = (Hashtable) tables.get(xid);
        if (xidTables != null) {
            synchronized (xidTables) {
                for (Object o : xidTables.entrySet()) {
                    Map.Entry entry = (Map.Entry) o;
                    RMTable xTable = (RMTable) entry.getValue();
                    RMTable table = getTable(xTable.getTablename());
                    for (Object key : xTable.keySet()) {
                        ResourceItem item = xTable.get(key);
                        if (item.isDeleted())
                            table.remove(item);
                        else
                            table.put(item);
                    }
                    if (!IOUtil.writeObject("data", entry.getKey().toString(), table)) {
                        throw new RemoteException("Can't write table to disk");
                    }
                    new File("data/" + xid + "/" + entry.getKey()).delete();
                }
                new File("data/" + xid).delete();
                tables.remove(xid);
            }
        }

        if (!lm.unlockAll(xid))
            throw new RuntimeException();

        synchronized (xids) {
            xids.remove(xid);
        }
    }

    public void abort(int xid) throws InvalidTransactionException, RemoteException {
        if (dieTime == DieTime.BEFORE_ABORT)
            dieNow();
        if (xid < 0) {
            throw new InvalidTransactionException(xid, "Xid must be positive.");
        }
        Hashtable xidTables = (Hashtable) tables.get(xid);
        if (xidTables != null) {
            synchronized (xidTables) {
                for (Object o : xidTables.entrySet()) {
                    Map.Entry entry = (Map.Entry) o;
                    new File("data/" + xid + "/" + entry.getKey()).delete();
                }
                new File("data/" + xid).delete();
                tables.remove(xid);
            }
        }

        if (!lm.unlockAll(xid))
            throw new RuntimeException();

        synchronized (xids) {
            xids.remove(xid);
        }
    }
}