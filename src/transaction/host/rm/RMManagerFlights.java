/*
 * Created on 2005-5-29
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package transaction.host.rm;

import transaction.core.Host;

import java.rmi.RemoteException;

public class RMManagerFlights extends ResourceManagerImpl<String> {
    public RMManagerFlights() throws RemoteException {
        super(Host.HostName.RMFlights);
    }

    public static void main(String[] args) {
        try {
            new RMManagerFlights().start();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}
