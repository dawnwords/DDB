package transaction;

import transaction.exception.InvalidTransactionException;
import util.IOUtil;
import util.Log;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Transaction Manager for the Distributed Travel Reservation System.
 * <p/>
 * Description: toy implementation of the TM
 */

public class TransactionManagerImpl extends Host implements TransactionManager {

    public static final String TM_LOG_FILE_NAME = "tm.log";
    private ConcurrentHashMap<Long, Queue<ResourceManager>> xidRMMap;
    private ConcurrentHashMap<Long, StateCounter> xidStateMap;
    private ReentrantReadWriteLock logLock;

    protected TransactionManagerImpl() throws RemoteException {
        super(HostName.TM);
        logLock = new ReentrantReadWriteLock();
    }

    public static void main(String args[]) {
        try {
            new TransactionManagerImpl().start();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        recover();
        bindRMIRegistry();
    }

    private void recover() {
        xidStateMap = loadLog();
        if (xidStateMap == null) {
            xidStateMap = new ConcurrentHashMap<Long, StateCounter>();
        }
        xidRMMap = new ConcurrentHashMap<Long, Queue<ResourceManager>>();
        for (Long xid : xidStateMap.keySet()) {
            xidRMMap.put(xid, new ConcurrentLinkedQueue<ResourceManager>());
        }
    }

    private ConcurrentHashMap<Long, StateCounter> loadLog() {
        logLock.readLock().lock();
        ConcurrentHashMap<Long, StateCounter> result = IOUtil.readObject(TM_LOG_FILE_NAME);
        logLock.readLock().unlock();
        return result;
    }

    private void storeLog() {
        logLock.writeLock().lock();
        IOUtil.writeObject(TM_LOG_FILE_NAME, xidStateMap);
        logLock.writeLock().unlock();
    }

    @Override
    public boolean dieNow() throws RemoteException {
        xidRMMap = null;
        xidStateMap = null;
        logLock = null;
        throw new RemoteException("TM died");
    }

    @Override
    public boolean reconnect() throws RemoteException {
        logLock = new ReentrantReadWriteLock();
        recover();
        return true;
    }

    @Override
    public boolean start(long xid) throws RemoteException {
        if (xidStateMap.get(xid) != null) {
            return false;
        }
        xidRMMap.put(xid, new ConcurrentLinkedQueue<ResourceManager>());
        xidStateMap.put(xid, new StateCounter());
        Log.i("start:%s", xidStateMap);
        return true;
    }

    @Override
    public boolean commit(long xid) throws RemoteException {
        if (dieTime == DieTime.BEFORE_COMMIT) {
            dieNow();
        }
        boolean result = prepare(xid) ? doCommit(xid) : abort(xid);
        if (dieTime == DieTime.AFTER_COMMIT) {
            dieNow();
        }
        return result;
    }

    private boolean prepare(long xid) {
        Queue<ResourceManager> rmList = xidRMMap.get(xid);
        StateCounter state = xidStateMap.get(xid);
        if (rmList == null || state == null) {
            throw new InvalidTransactionException(xid, "no such xid to prepare to commit");
        }

        if (state.state != State.Start) {
            throw new IllegalStateException(String.format("Illegal State: %s, for prepare phase for %d", state, xid));
        }

        state.state(State.Prepare);
        try {
            for (ResourceManager resourceManager : rmList) {
                if (resourceManager.prepare(xid)) {
                    Log.i("Prepare for %d:[%s]%s", xid, state, resourceManager);
                    if (state.increaseAndCheck(rmList.size())) {
                        state.state(State.Commit);
                        return true;
                    }
                }
            }
        } catch (Exception ignored) {
        }
        state.state(State.Abort);
        return false;
    }

    private boolean doCommit(long xid) {
        Queue<ResourceManager> rmList = xidRMMap.get(xid);
        StateCounter state = xidStateMap.get(xid);
        if (rmList == null || state == null) {
            throw new InvalidTransactionException(xid, "no such xid to commit");
        }

        if (state.state != State.Commit) {
            throw new IllegalStateException(String.format("Illegal State: %s, for commit phase for %d", state, xid));
        }

        for (ResourceManager resourceManager : rmList) {
            try {
                resourceManager.commit(xid);
                Log.i("Commit for %d:[%s]%s", xid, state, resourceManager);
                if (state.increaseAndCheck(rmList.size())) {
                    state.state(State.Finish);
                    return true;
                }
            } catch (RemoteException ignored) {
            }
        }
        return false;
    }

    @Override
    public boolean abort(long xid) throws RemoteException {
        Queue<ResourceManager> rmList = xidRMMap.get(xid);
        StateCounter state = xidStateMap.get(xid);
        if (rmList == null || state == null) {
            throw new InvalidTransactionException(xid, "no such xid to abort");
        }

        if (state.state != State.Abort) {
            throw new InvalidTransactionException(xid, String.format("Illegal State: %s, for abort phase for %d", state, xid));
        }

        for (ResourceManager resourceManager : rmList) {
            try {
                resourceManager.abort(xid);
                if (state.increaseAndCheck(rmList.size())) {
                    state.state(State.Finish);
                    return true;
                }
            } catch (RemoteException ignored) {
            }
        }
        return false;
    }

    public void enlist(long xid, ResourceManager rm) throws RemoteException {
        Queue<ResourceManager> rms = xidRMMap.get(xid);
        StateCounter state = xidStateMap.get(xid);
        if (rms == null || state == null) {
            throw new InvalidTransactionException(xid, "enlist no such xid:" + xid);
        }
        if (state.state != State.Start) {
            throw new InvalidTransactionException(xid, String.format("Illegal State: %s, for enlist %d", state.state, xid));
        }
        if (!rms.contains(rm)) {
            rms.add(rm);
        }
    }

    private enum State {
        Start, Prepare, Commit, Abort, Finish
    }

    private class StateCounter implements Serializable {
        State state;
        AtomicInteger count;

        StateCounter() {
            state = State.Start;
            count = new AtomicInteger(0);
        }

        boolean increaseAndCheck(int limit) {
            boolean result = count.incrementAndGet() >= limit;
            storeLog();
            return result;
        }

        void state(State state) {
            this.state = state;
            count.set(0);
            storeLog();
        }

        @Override
        public String toString() {
            return "StateCounter{" +
                    "state=" + state +
                    ", count=" + count +
                    '}';
        }
    }
}
