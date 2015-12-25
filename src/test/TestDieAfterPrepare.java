package test;

import transaction.Host;
import transaction.exception.TransactionAbortedException;

/**
 * Created by Dawnwords on 2015/12/25.
 */
public class TestDieAfterPrepare extends BaseClient {
    public static void main(String[] args) {
        new TestDieAfterPrepare().test();
    }

    @Override
    public void run() {
        try {
            wc().dieRMAfterPrepare(Host.HostName.RMReservations);
            long xid = wc().start();
            assertTrue("Add flight", wc().addFlight(xid, "377", 230, 999));
            assertTrue("Add room", wc().addRooms(xid, "SFR", 500, 150));
            assertEqual("Check Flight Seat Number", wc().queryFlight(xid, "377"), 230);
            assertTrue("Add customer", wc().newCustomer(xid, "Christ"));
            wc().commit(xid);

            xid = wc().start();
            assertTrue("Reserve flight", wc().reserveFlight(xid, "Christ", "377"));
            assertTrue("Reserve room", wc().reserveRoom(xid, "Christ", "SFR"));
            assertEqual("Check Flight Seat Remaining", wc().queryFlight(xid, "377"), 229);
            assertEqual("Check Room Remaining", wc().queryRooms(xid, "SFR"), 499);
            try {
                wc().commit(xid);
                assertTrue("Commit Must Fail", false);
            } catch (TransactionAbortedException e) {
                System.out.println("Transaction Abort");
                assertTrue("WC reconnect", wc().reconnect());
            }

            xid = wc().start();
            assertEqual("Check Room Remaining", wc().queryRooms(xid, "SFR"), 500);
            assertEqual("Check Flight Seat Remaining", wc().queryFlight(xid, "377"), 230);
            wc().commit(xid);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
