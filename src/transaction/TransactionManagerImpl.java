package transaction;

import transaction.exception.IllegalTransactionStateException;
import transaction.exception.InvalidTransactionException;
import transaction.exception.TransactionAbortedException;
import transaction.exception.TransactionManagerUnaccessibleException;
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
        xidRMMap.clear();
        xidStateMap.clear();
        throw new TransactionManagerUnaccessibleException();
    }

    @Override
    public boolean reconnect() throws RemoteException {
        dieTime = DieTime.NO_DIE;
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
    public boolean commit(long xid) throws RemoteException, TransactionAbortedException {
        if (dieTime == DieTime.BEFORE_COMMIT) {
            dieNow();
        }
        if (prepare(xid)) {
            end(xid, State.Commit);
            if (dieTime == DieTime.AFTER_COMMIT) {
                dieNow();
            }
            return true;
        } else {
            end(xid, State.Abort);
            throw new TransactionAbortedException(xid, "Prepare Failed");
        }
    }

    private boolean prepare(long xid) {
        Queue<ResourceManager> rmList = xidRMMap.get(xid);
        StateCounter state = xidStateMap.get(xid);
        if (rmList == null || state == null) {
            throw new InvalidTransactionException(xid, "no such xid to prepare to commit");
        }

        if (state.state != State.Start) {
            return false;
        }

        int expect = rmList.size();
        state.state(State.Prepare, expect);
        try {
            for (ResourceManager resourceManager : rmList) {
                if (resourceManager.prepare(xid)) {
                    Log.i("Prepare for %d:[%s]%s", xid, state, resourceManager.hostName());
                    if (state.increaseAndCheck()) {
                        state.state(State.Commit, expect);
                        return true;
                    }
                }
            }
        } catch (Exception ignored) {
        }
        state.state(State.Abort, expect);
        return false;
    }

    @Override
    public boolean abort(long xid) throws RemoteException {
        Queue<ResourceManager> rmList = xidRMMap.get(xid);
        StateCounter state = xidStateMap.get(xid);
        if (rmList == null || state == null) {
            throw new InvalidTransactionException(xid, "no such xid");
        }
        state.state(State.Abort, rmList.size());
        return end(xid, State.Abort);
    }

    private boolean end(long xid, State currentState) {
        Queue<ResourceManager> rmList = xidRMMap.get(xid);
        StateCounter state = xidStateMap.get(xid);
        if (rmList == null || state == null) {
            throw new InvalidTransactionException(xid, "no such xid to " + currentState);
        }

        if (state.state != currentState) {
            Log.e("Illegal State[%s] for xid:%d", state, xid);
            return false;
        }

        for (ResourceManager rm : rmList) {
            if (end(xid, rm, state)) {
                return true;
            }
        }
        return false;
    }

    private boolean end(long xid, ResourceManager rm, StateCounter state) {
        try {
            if (state.state == State.Commit) {
                rm.commit(xid);
                Log.i("Commit for %d:%s", xid, state, rm.hostName());
            } else {
                rm.abort(xid);
                Log.i("Abort for %d:%s", xid, state, rm.hostName());
            }

            if (state.increaseAndCheck()) {
                state.state(State.Finish, 0);
                return true;
            }
        } catch (Exception e) {
            Log.e(e.getMessage());
        }
        return false;
    }

    @Override
    public void enlist(long xid, ResourceManager rm) throws RemoteException, IllegalTransactionStateException {
        Queue<ResourceManager> rms = xidRMMap.get(xid);
        StateCounter state = xidStateMap.get(xid);
        if (rms == null || state == null) {
            throw new InvalidTransactionException(xid, "enlist no such xid:" + xid);
        }
        switch (state.state) {
            case Start:
                if (!rms.contains(rm)) {
                    rms.add(rm);
                }
                return;
            case Commit:
            case Abort:
                end(xid, rm, state);
                return;
            default:
                throw new IllegalTransactionStateException(xid, state.state, "enlist");
        }

    }

    private class StateCounter implements Serializable {
        State state;
        AtomicInteger count;
        int expect;

        StateCounter() {
            state = State.Start;
            count = new AtomicInteger(0);
        }

        boolean increaseAndCheck() {
            boolean result = count.incrementAndGet() >= expect;
            storeLog();
            return result;
        }

        void state(State state, int expect) {
            this.expect = expect;
            this.state = state;
            count.set(0);
            storeLog();
        }

        @Override
        public String toString() {
            return "StateCounter{" +
                    "state=" + state +
                    ", count=" + count +
                    ", expect=" + expect +
                    '}';
        }
    }
}
