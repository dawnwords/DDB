package transaction;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

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

    public abstract boolean reconnect() throws RemoteException;

    public enum HostName {
        RMFlights, RMRooms, RMCars, RMReservations, TM, WC, ALL
    }
}
