/*
 * Created on 2005-5-18
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package transaction.exception;

import transaction.core.Host;

/**
 * @author RAdmin
 *         <p/>
 *         TODO To change the template for this generated type comment go to
 *         Window - Preferences - Java - Code Style - Code Templates
 */
public class TransactionManagerUnaccessibleException extends UnaccessibleException {
    public TransactionManagerUnaccessibleException() {
        super(Host.HostName.TM);
    }
}
