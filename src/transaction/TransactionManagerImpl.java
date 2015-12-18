package transaction;

import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;

/**
 * Transaction Manager for the Distributed Travel Reservation System.
 * <p/>
 * Description: toy implementation of the TM
 */

public class TransactionManagerImpl extends Host implements TransactionManager {

    protected TransactionManagerImpl() throws RemoteException {
        super(HostName.TM);
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
        //TODO finish
        return false;
    }

    @Override
    public boolean start(int xid) throws RemoteException {
        return false;
    }

    @Override
    public boolean commit(int xid) throws RemoteException {
        return false;
    }

    @Override
    public boolean abort(int xid) throws RemoteException {
        return false;
    }

    public void enlist(int xid, ResourceManager rm) throws RemoteException {
    }

}
