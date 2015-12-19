package transaction;

import java.rmi.RemoteException;

/**
 * Created by Dawnwords on 2015/12/18.
 */
public class RMManagerCustomers extends ResourceManagerImpl<String> {
    public RMManagerCustomers() throws RemoteException {
        super(HostName.RMCustomers);
    }

    public static void main(String[] args) {
        try {
            new RMManagerCustomers().start();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}
