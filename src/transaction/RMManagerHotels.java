/*
 * Created on 2005-5-29
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package transaction;

import java.rmi.RemoteException;

public class RMManagerHotels {
    public static void main(String[] args) {
        try {
            new ResourceManagerImpl<String>(Host.HostName.RMRooms).start();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}
