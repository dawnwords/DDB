package test;

import transaction.Host;
import transaction.exception.TransactionAbortedException;

/**
 * Created by Dawnwords on 2015/12/24.
 */
public class TestDieAfterEnlist extends BaseClient {

    public static void main(String[] args) {
        new TestDieAfterEnlist().run();
    }

    @Override
    protected void setUp() throws Exception {
        wc().dieRMAfterEnlist(Host.HostName.RMReservations);
    }

    @Override
    protected void run(long xid) throws Exception {
        assertTrue("Add flight", wc().addFlight(xid, "347", 230, 999));
        assertTrue("Add room", wc().addRooms(xid, "SFO", 500, 150));
        assertEqual("Check Flight Seat Number", wc().queryFlight(xid, "347"), 230);
        assertTrue("Add customer", wc().newCustomer(xid, "John"));
        wc().commit(xid);

        xid = wc().start();
        assertTrue("Reserve flight", wc().reserveFlight(xid, "John", "347"));
        assertEqual("Check Flight Seat Number", wc().queryFlight(xid, "347"), 229);
        try {
            wc().commit(xid);
            assertTrue("Commit Must Fail", false);
        } catch (TransactionAbortedException e) {
            System.out.println("Transaction Abort");
            assertTrue("WC reconnect", wc().reconnect());
        }

        xid = wc().start();
        assertEqual("Check Reservation", wc().queryFlight(xid, "347"), 230);
        wc().commit(xid);
    }
}
