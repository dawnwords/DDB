package test;

import transaction.Host;

import java.rmi.RemoteException;

/**
 * Created by Dawnwords on 2015/12/26.
 */
public class TestAllDie extends TestClient {
    public static void main(String[] args) {
        new TestAllDie().test();
    }

    @Override
    protected void run() {
        try {
            long xid = wc().start();
            assertTrue("Add flight", wc().addFlight(xid, "427", 230, 999));
            assertTrue("Add room", wc().addRooms(xid, "SFW", 500, 150));
            assertEqual("Check Flight Seat Number", wc().queryFlight(xid, "427"), 230);
            assertTrue("Add customer", wc().newCustomer(xid, "Harry"));
            assertTrue("Reserve Flight", wc().reserveFlight(xid, "Harry", "427"));
            assertTrue("All Die", wc().dieNow(Host.HostName.ALL));
            try {
                wc().commit(xid);
                assertTrue("Commit Must Fail", false);
            } catch (RemoteException e) {
                System.out.println("Transaction Abort");
                assertTrue("WC reconnect", wc().reconnect());
            }

            Thread.sleep(2000);

            xid = wc().start();
            assertTrue("No Flight Add", wc().queryFlight(xid, "427") == -1);
            assertTrue("No Room Add", wc().queryRooms(xid, "SFW") == -1);
            assertTrue("No Customer And Reservation Add", wc().queryCustomerBill(xid, "Harry") == -1);
            assertTrue("Commmit", wc().commit(xid));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
