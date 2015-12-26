package test;

import java.rmi.RemoteException;

/**
 * Created by Dawnwords on 2015/12/25.
 */
public class TestTMDieAfterCommit extends TestClient {
    public static void main(String[] args) {
        new TestTMDieAfterCommit().test();
    }

    @Override
    public void run() {
        try {
            long xid = wc().start();
            assertTrue("Add flight", wc().addFlight(xid, "417", 230, 999));
            assertTrue("Add room", wc().addRooms(xid, "SFV", 500, 150));
            assertEqual("Check Flight Seat Number", wc().queryFlight(xid, "417"), 230);
            assertTrue("Add customer", wc().newCustomer(xid, "George"));
            assertTrue("Commit", wc().commit(xid));

            wc().dieTMAfterCommit();

            xid = wc().start();
            assertTrue("Reserve flight", wc().reserveFlight(xid, "George", "417"));
            assertTrue("Reserve room", wc().reserveRoom(xid, "George", "SFV"));
            assertEqual("Check Flight Seat Remaining", wc().queryFlight(xid, "417"), 229);
            assertEqual("Check Room Remaining", wc().queryRooms(xid, "SFV"), 499);
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
            assertEqual("Check Room Remaining", wc().queryRooms(xid, "SFV"), 499);
            assertEqual("Check Flight Seat Remaining", wc().queryFlight(xid, "417"), 229);
            wc().commit(xid);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
