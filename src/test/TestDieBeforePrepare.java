package test;

import transaction.Host;
import transaction.exception.TransactionAbortedException;

/**
 * Created by Dawnwords on 2015/12/25.
 */
public class TestDieBeforePrepare extends TestClient {
    public static void main(String[] args) {
        new TestDieBeforePrepare().test();
    }

    @Override
    public void run() {
        try {
            wc().dieRMBeforePrepare(Host.HostName.RMReservations);
            long xid = wc().start();
            assertTrue("Add flight", wc().addFlight(xid, "367", 230, 999));
            assertTrue("Add room", wc().addRooms(xid, "SFQ", 500, 150));
            assertEqual("Check Flight Seat Number", wc().queryFlight(xid, "367"), 230);
            assertTrue("Add customer", wc().newCustomer(xid, "Bob"));
            wc().commit(xid);

            xid = wc().start();
            assertTrue("Reserve flight", wc().reserveFlight(xid, "Bob", "367"));
            assertTrue("Reserve room", wc().reserveRoom(xid, "Bob", "SFQ"));
            assertEqual("Check Flight Seat Remaining", wc().queryFlight(xid, "367"), 229);
            assertEqual("Check Room Remaining", wc().queryRooms(xid, "SFQ"), 499);
            try {
                wc().commit(xid);
                assertTrue("Commit Must Fail", false);
            } catch (TransactionAbortedException e) {
                System.out.println("Transaction Abort");
                assertTrue("WC reconnect", wc().reconnect());
            }

            xid = wc().start();
            assertEqual("Check Room Remaining", wc().queryRooms(xid, "SFQ"), 500);
            assertEqual("Check Flight Seat Remaining", wc().queryFlight(xid, "367"), 230);
            wc().commit(xid);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
