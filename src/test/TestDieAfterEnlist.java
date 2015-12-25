package test;

import transaction.Host;
import transaction.exception.TransactionAbortedException;

/**
 * Created by Dawnwords on 2015/12/24.
 */
public class TestDieAfterEnlist extends BaseClient {

    public static void main(String[] args) {
        new TestDieAfterEnlist().test();
    }


    @Override
    public void test() {
        try {
            wc().dieRMAfterEnlist(Host.HostName.RMReservations);
            long xid = wc().start();
            assertTrue("Add flight", wc().addFlight(xid, "357", 230, 999));
            assertTrue("Add room", wc().addRooms(xid, "SFP", 500, 150));
            assertEqual("Check Flight Seat Number", wc().queryFlight(xid, "357"), 230);
            assertTrue("Add customer", wc().newCustomer(xid, "Alice"));
            wc().commit(xid);

            xid = wc().start();
            try {
                wc().reserveFlight(xid, "Alice", "357");
                assertTrue("Reserve flight Must Fail", false);
            } catch (TransactionAbortedException e) {
                System.out.println("Transaction Abort");
                assertTrue("WC reconnect", wc().reconnect());
            }

            xid = wc().start();
            assertEqual("Check Seat Remaining", wc().queryFlight(xid, "357"), 230);
            wc().commit(xid);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
