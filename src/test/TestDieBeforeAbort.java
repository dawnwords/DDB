package test;

import transaction.Host;

/**
 * Created by Dawnwords on 2015/12/25.
 */
public class TestDieBeforeAbort extends BaseClient {
    public static void main(String[] args) {
        new TestDieBeforeAbort().test();
    }

    @Override
    public void run() {
        try {
            wc().dieRMBeforeAbort(Host.HostName.RMReservations);
            long xid = wc().start();
            assertTrue("Add flight", wc().addFlight(xid, "397", 230, 999));
            assertTrue("Add room", wc().addRooms(xid, "SFT", 500, 150));
            assertEqual("Check Flight Seat Number", wc().queryFlight(xid, "397"), 230);
            assertTrue("Add customer", wc().newCustomer(xid, "Ellen"));
            wc().commit(xid);

            xid = wc().start();
            assertTrue("Reserve flight", wc().reserveFlight(xid, "Ellen", "397"));
            assertTrue("Reserve room", wc().reserveRoom(xid, "Ellen", "SFT"));
            assertEqual("Check Flight Seat Remaining", wc().queryFlight(xid, "397"), 229);
            assertEqual("Check Room Remaining", wc().queryRooms(xid, "SFT"), 499);
            wc().abort(xid);
            System.out.println("Transaction Abort");
            assertTrue("WC reconnect", wc().reconnect());

            xid = wc().start();
            assertEqual("Check Room Remaining", wc().queryRooms(xid, "SFT"), 500);
            assertEqual("Check Flight Seat Remaining", wc().queryFlight(xid, "397"), 230);
            wc().commit(xid);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
