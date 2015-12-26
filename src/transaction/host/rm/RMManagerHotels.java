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
 * The RM of Hotels
 * <p/>
 * Created by Dawnwords on 2015/12/18.
 */
public class RMManagerHotels extends ResourceManagerImpl<String> {
    public RMManagerHotels() throws RemoteException {
        super(Host.HostName.RMRooms);
    }

    public static void main(String[] args) {
        try {
            new RMManagerHotels().start();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}
