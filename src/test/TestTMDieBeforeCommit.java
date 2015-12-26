package test;

import java.rmi.RemoteException;

/**
 * Created by Dawnwords on 2015/12/25.
 */
public class TestTMDieBeforeCommit extends TestClient {
    public static void main(String[] args) {
        new TestTMDieBeforeCommit().test();
    }

    @Override
    public void run() {
        try {
            long xid = wc().start();
            assertTrue("Add flight", wc().addFlight(xid, "407", 230, 999));
            assertTrue("Add room", wc().addRooms(xid, "SFU", 500, 150));
            assertEqual("Check Flight Seat Number", wc().queryFlight(xid, "407"), 230);
            assertTrue("Add customer", wc().newCustomer(xid, "Frank"));
            assertTrue("Commit", wc().commit(xid));

            wc().dieTMBeforeCommit();

            xid = wc().start();
            assertTrue("Reserve flight", wc().reserveFlight(xid, "Frank", "407"));
            assertTrue("Reserve room", wc().reserveRoom(xid, "Frank", "SFU"));
            assertEqual("Check Flight Seat Remaining", wc().queryFlight(xid, "407"), 229);
            assertEqual("Check Room Remaining", wc().queryRooms(xid, "SFU"), 499);
            try {
                wc().commit(xid);
                assertTrue("Commit Must Fail", false);
            } catch (RemoteException e) {
                System.out.println("Transaction Commit Fail For TM Died. Wait for 2s to notice RM");
                Thread.sleep(2000);
                assertTrue("WC reconnect", wc().reconnect());
            }

            System.out.println("After Reconnection, Former Transaction Roll Forward.");
            xid = wc().start();
            assertEqual("Check Room Remaining", wc().queryRooms(xid, "SFU"), 499);
            assertEqual("Check Flight Seat Remaining", wc().queryFlight(xid, "407"), 229);
            wc().commit(xid);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
