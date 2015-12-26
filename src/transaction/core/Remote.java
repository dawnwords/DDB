package transaction.core;

import java.rmi.RemoteException;

/**
 * Basic Interface containing methods for each Host to implement
 * <p/>
 * Created by Dawnwords on 2015/12/22.
 */
public interface Remote extends java.rmi.Remote {
    /**
     * Remote clear all its states and pretend to be shut down
     *
     * @return true if Remote die successfully, false otherwise
     * @throws RemoteException
     */
    boolean dieNow() throws RemoteException;

    /**
     * Invoker call this method to make Remote recover its states and get reconnected
     *
     * @return true if Remote is reconnected successfully, false otherwise
     * @throws RemoteException
     */
    boolean reconnect() throws RemoteException;

    /**
     * Invoker call this method to check if Remote is alive
     *
     * @throws RemoteException if Remote is shut down or <code>dieNow()</code> is invoked.
     */
    void ping() throws RemoteException;

    /**
     * Invoker call this method to set a <code>DieTime</code> to Remote.
     * After a <code>DieTime</code> is set, Remote call <code>dieNow()</code> at this given time.
     *
     * @param dieTime the <code>DieTime</code> when the Remote call <code>dieNow()</code>.
     * @throws RemoteException
     */
    void setDieTime(DieTime dieTime) throws RemoteException;

    /**
     * @return the host name of Remote
     * @throws RemoteException
     */
    String hostName() throws RemoteException;
}
