package transaction;

import transaction.exception.InvalidTransactionException;

import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Transaction Manager for the Distributed Travel Reservation System.
 * <p/>
 * Description: toy implementation of the TM
 */

public class TransactionManagerImpl extends Host implements TransactionManager {

    private ConcurrentHashMap<Long, List<ResourceManager>> xidRMMap;
    private ConcurrentHashMap<Long, StateCounter> xidStateMap;

    protected TransactionManagerImpl() throws RemoteException {
        super(HostName.TM);
        xidRMMap = new ConcurrentHashMap<Long, List<ResourceManager>>();
        xidStateMap = new ConcurrentHashMap<Long, StateCounter>();
    }

    public static void main(String args[]) {
        System.setSecurityManager(new RMISecurityManager());

        String rmiPort = System.getProperty("rmiPort");
        if (rmiPort == null) {
            rmiPort = "";
        } else if (!rmiPort.equals("")) {
            rmiPort = "//:" + rmiPort + "/";
        }

        try {
            TransactionManagerImpl obj = new TransactionManagerImpl();
            Naming.rebind(rmiPort + Host.HostName.TM, obj);
            System.out.println("TM bound");
        } catch (Exception e) {
            System.err.println("TM not bound:" + e);
        }
    }

    @Override
    public boolean reconnect() throws RemoteException {
        return true;
    }

    @Override
    public boolean start(long xid) throws RemoteException {
        if (xidRMMap.get(xid) != null) {
            return false;
        }
        xidRMMap.put(xid, new ArrayList<ResourceManager>());
        xidStateMap.put(xid, new StateCounter());
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
        List<ResourceManager> rmList = xidRMMap.get(xid);
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
                    if (state.increaseAndCheck()) {
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
        List<ResourceManager> rmList = xidRMMap.get(xid);
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
                if (state.increaseAndCheck()) {
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
        List<ResourceManager> rmList = xidRMMap.get(xid);
        StateCounter state = xidStateMap.get(xid);
        if (rmList == null || state == null) {
            throw new InvalidTransactionException(xid, "no such xid to abort");
        }

        if (state.state != State.Commit) {
            throw new IllegalStateException(String.format("Illegal State: %s, for abort phase for %d", state, xid));
        }

        for (ResourceManager resourceManager : rmList) {
            try {
                resourceManager.abort(xid);
                if (state.increaseAndCheck()) {
                    state.state(State.Finish);
                    return true;
                }
            } catch (RemoteException ignored) {
            }
        }
        return false;
    }

    public void enlist(long xid, ResourceManager rm) throws RemoteException {

    }

    private enum State {
        Start, Prepare, Commit, Abort, Finish
    }

    private class StateCounter {
        State state;
        AtomicInteger count;

        StateCounter() {
            state = State.Start;
            count = new AtomicInteger(0);
        }

        boolean increaseAndCheck() {
            boolean result = count.incrementAndGet() >= 4;
            //TODO write log
            return result;
        }

        void state(State state) {
            this.state = state;
            //TODO write log
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
