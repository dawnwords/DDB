package transaction;

import java.rmi.RemoteException;

/**
 * Created by Dawnwords on 2015/12/22.
 */
public interface Remote extends java.rmi.Remote {
    boolean dieNow() throws RemoteException;

    void ping() throws RemoteException;

    void setDieTime(DieTime dieTime) throws RemoteException;

    boolean reconnect() throws RemoteException;
}
