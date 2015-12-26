package test;

import transaction.Host;
import transaction.exception.TransactionAbortedException;

/**
 * Created by Dawnwords on 2015/12/26.
 */
public class TestRMDie extends TestClient {
    public static void main(String[] args) {
        new TestRMDie().test();
    }

    @Override
    protected void run() {
        try {
            long xid = wc().start();
            assertTrue("Add customer", wc().newCustomer(xid, "Isac"));
            assertTrue("Commit", wc().commit(xid));

            xid = wc().start();
            assertTrue("Add flight", wc().addFlight(xid, "437", 230, 999));
            assertTrue("Add room", wc().addRooms(xid, "SFX", 500, 150));
            assertEqual("Check Flight Seat Number", wc().queryFlight(xid, "437"), 230);
            assertTrue("Reserve Flight", wc().reserveFlight(xid, "Isac", "437"));
            assertTrue("RMReservations Die", wc().dieNow(Host.HostName.RMReservations));
            try {
                wc().commit(xid);
                assertTrue("Commit Must Fail", false);
            } catch (TransactionAbortedException e) {
                System.out.println("Transaction Abort");
                assertTrue("WC reconnect", wc().reconnect());
            }

            Thread.sleep(2000);

            xid = wc().start();
            assertTrue("No Flight Add", wc().queryFlight(xid, "437") == -1);
            assertTrue("No Room Add", wc().queryRooms(xid, "SFX") == -1);
            assertTrue("No Reservation Add", wc().queryCustomerBill(xid, "Isac") == 0);
            assertTrue("Commit", wc().commit(xid));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
