/*
 * Created on 2005-5-29
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package transaction;

import java.rmi.RemoteException;

public class RMManagerCars {
    public static void main(String[] args) {
        try {
            new ResourceManagerImpl<String>(ResourceManager.RMINameCars).start();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}