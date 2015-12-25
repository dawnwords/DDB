package transaction.exception;

import transaction.Host;

import java.rmi.RemoteException;

/**
 * Created by Dawnwords on 2015/12/25.
 */
public class UnaccessibleException extends RemoteException {
    public UnaccessibleException(Host.HostName who) {
        super("Unaccessible to " + who.name());
    }
}
