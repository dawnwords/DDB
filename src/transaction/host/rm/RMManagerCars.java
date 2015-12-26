/*
 * Created on 2005-5-29
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package transaction.host.rm;

import transaction.core.Host;

import java.rmi.RemoteException;

/**
 * The RM of Cars.
 * <p/>
 * Created by Dawnwords on 2015/12/18.
 */
public class RMManagerCars extends ResourceManagerImpl<String> {
    public RMManagerCars() throws RemoteException {
        super(Host.HostName.RMCars);
    }

    public static void main(String[] args) {
        try {
            new RMManagerCars().start();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}