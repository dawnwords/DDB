package test;

import transaction.Host;

/**
 * Created by Dawnwords on 2015/12/25.
 */
public class TestDieBeforeCommit extends TestClient {
    public static void main(String[] args) {
        new TestDieBeforeCommit().test();
    }

    @Override
    public void run() {
        try {
            wc().dieRMBeforeCommit(Host.HostName.RMReservations);
            long xid = wc().start();
            assertTrue("Add flight", wc().addFlight(xid, "387", 230, 999));
            assertTrue("Add room", wc().addRooms(xid, "SFS", 500, 150));
            assertEqual("Check Flight Seat Number", wc().queryFlight(xid, "387"), 230);
            assertTrue("Add customer", wc().newCustomer(xid, "Dave"));
            wc().commit(xid);

            xid = wc().start();
            assertTrue("Reserve flight", wc().reserveFlight(xid, "Dave", "387"));
            assertTrue("Reserve room", wc().reserveRoom(xid, "Dave", "SFS"));
            assertEqual("Check Flight Seat Remaining", wc().queryFlight(xid, "387"), 229);
            assertEqual("Check Room Remaining", wc().queryRooms(xid, "SFS"), 499);
            wc().commit(xid);
            System.out.println("Transaction Commit");
            assertTrue("WC reconnect", wc().reconnect());

            xid = wc().start();
            assertEqual("Check Room Remaining", wc().queryRooms(xid, "SFS"), 499);
            assertEqual("Check Flight Seat Remaining", wc().queryFlight(xid, "387"), 229);
            wc().commit(xid);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
