package transaction.core;

import transaction.exception.ResourceManagerUnaccessibleException;
import transaction.exception.TransactionManagerUnaccessibleException;
import util.Log;

import java.io.FileInputStream;
import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Properties;

/**
 * This abstract class gives the default implementations of <code>ping()</code>, <code>setDateTime()</code>,
 * and <code>hostName()</code>, as well as some common functions for hosts, i.e. RMs, TM and WC
 * <p/>
 * Created by Dawnwords on 2015/12/18.
 */
public abstract class Host extends UnicastRemoteObject {
    protected HostName myRMIName;
    protected DieTime dieTime;
    protected volatile boolean hasDead;

    protected Host(HostName rmiName) throws RemoteException {
        dieTime = DieTime.NO_DIE;
        myRMIName = rmiName;
    }

    public void ping() throws RemoteException {
        if (hasDead) {
            throw myRMIName == HostName.TM ?
                    new TransactionManagerUnaccessibleException() :
                    new ResourceManagerUnaccessibleException(myRMIName);
        }
    }

    public void setDieTime(DieTime dieTime) throws RemoteException {
        this.dieTime = dieTime;
        Log.i("Die time set to : " + dieTime);
    }

    public String hostName() {
        return myRMIName.name();
    }

    protected void bindRMIRegistry() {
        Properties properties = loadProperties();
        try {
            LocateRegistry.createRegistry(getPort(properties, myRMIName));
            Naming.rebind(getRMI(properties, myRMIName), this);
            Log.i(myRMIName + " bound");
        } catch (Exception e) {
            throw new RuntimeException(myRMIName + " not bound:" + e);
        }
    }

    protected Remote lookUp(HostName who) {
        Properties properties = loadProperties();
        try {
            return Naming.lookup(getRMI(properties, who));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String getRMI(Properties prop, HostName who) {
        return String.format("rmi://%s:%d/%s",
                prop.getProperty(who.ip()),
                getPort(prop, who),
                who.name());
    }

    private int getPort(Properties prop, HostName who) {
        return Integer.parseInt(prop.getProperty(who.port()));
    }

    private Properties loadProperties() {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream("conf/ddb.conf"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return prop;
    }

    /**
     * The Host Name Enumerator for RMs, TM, and WC
     */
    public enum HostName {
        RMFlights("rm.RMFlights"),
        RMRooms("rm.RMRooms"),
        RMCars("rm.RMCars"),
        RMReservations("rm.RMReservations"),
        RMCustomers("rm.RMCustomers"),
        TM("tm"),
        WC("wc"),
        ALL(null);

        private final String name;

        HostName(String name) {
            this.name = name;
        }

        public String port() {
            return name + ".port";
        }

        public String ip() {
            return name + ".ip";
        }
    }
}
