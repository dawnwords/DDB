/*
 * Created on 2005-5-29
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package transaction;

import transaction.bean.ReservationKey;

import java.rmi.RemoteException;

public class RMManagerReservations extends ResourceManagerImpl<ReservationKey> {
    public RMManagerReservations() throws RemoteException {
        super(Host.HostName.RMReservations);
    }

    public static void main(String[] args) {
        try {
            new RMManagerReservations().start();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}
