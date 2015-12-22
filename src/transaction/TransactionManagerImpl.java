package transaction;

import transaction.exception.InvalidTransactionException;

import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Transaction Manager for the Distributed Travel Reservation System.
 * <p/>
 * Description: toy implementation of the TM
 */

public class TransactionManagerImpl extends Host implements TransactionManager {

    private ConcurrentHashMap<Long, List<ResourceManager>> rmMap;

    protected TransactionManagerImpl() throws RemoteException {
        super(HostName.TM);
        rmMap = new ConcurrentHashMap<Long, List<ResourceManager>>();
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
        if (rmMap.get(xid) != null) {
            return false;
        }
        rmMap.put(xid, new ArrayList<ResourceManager>());
        return true;
    }

    @Override
    public boolean commit(long xid) throws RemoteException {
        List<ResourceManager> rmList = rmMap.get(xid);
        if (rmList == null) {
            return false;
        }
        for (ResourceManager resourceManager : rmList) {
            try {
                resourceManager.prepare(xid);
            } catch (InvalidTransactionException e) {

            }
        }
        return false;
    }

    @Override
    public boolean abort(long xid) throws RemoteException {
        return false;
    }

    public void enlist(long xid, ResourceManager rm) throws RemoteException {

    }

}
