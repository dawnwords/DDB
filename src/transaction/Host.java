package transaction;

import java.io.FileInputStream;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Properties;

/**
 * Created by Dawnwords on 2015/12/18.
 */
public abstract class Host extends UnicastRemoteObject {
    protected HostName myRMIName;
    protected DieTime dieTime;

    protected Host(HostName rmiName) throws RemoteException {
        dieTime = DieTime.NO_DIE;
        myRMIName = rmiName;
    }

    public boolean dieNow() throws RemoteException {
        System.exit(1);
        return true;
    }

    public void ping() throws RemoteException {
    }

    public void setDieTime(DieTime dieTime) throws RemoteException {
        this.dieTime = dieTime;
        System.out.println("Die time set to : " + dieTime);
    }

    protected void bindRMIRegistry() {
        try {
            Registry rmiRegistry = LocateRegistry.createRegistry(Integer.parseInt(getConfig(myRMIName.port)));
            rmiRegistry.bind(myRMIName.name(), this);
            System.out.println(myRMIName + " bound");
        } catch (Exception e) {
            throw new RuntimeException(myRMIName + " not bound:" + e);
        }
    }

    protected String getConfig(String port) {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream("conf/ddb.conf"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return prop.getProperty(port);
    }

    protected Host lookUp(HostName who) {
        String rmiPort = System.getProperty("rmiPort");
        if (rmiPort == null) {
            rmiPort = "";
        } else if (!rmiPort.equals("")) {
            rmiPort = "//:" + rmiPort + "/";
        }

        try {
            return (Host) Naming.lookup(rmiPort + who);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public abstract boolean reconnect() throws RemoteException;

    public enum HostName {
        RMFlights("rm.RMFlights.port"),
        RMRooms("rm.RMRooms.port"),
        RMCars("rm.RMCars.port"),
        RMReservations("rm.RMReservations.port"),
        RMCustomers("rm.RMCustomers.port"),
        TM("tm.port"),
        WC("wc.port"),
        ALL(null);

        private final String port;

        HostName(String port) {
            this.port = port;
        }

        public String port() {
            return port;
        }
    }
}
