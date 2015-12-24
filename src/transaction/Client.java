package transaction;

import test.BaseClient;

/**
 * A toy client of the Distributed Travel Reservation System.
 */

public class Client extends BaseClient {

    public static void main(String args[]) {
        new Client().run();
    }

    @Override
    protected void run(long xid) throws Exception {
        assertTrue("Add flight", wc().addFlight(xid, "347", 230, 999));
        assertTrue("Add room", wc().addRooms(xid, "SFO", 500, 150));
        assertEqual("Check Flight Seat Number", wc().queryFlight(xid, "347"), 230);
        assertTrue("Add customer", wc().newCustomer(xid, "John"));
        assertTrue("Reserve flight", wc().reserveFlight(xid, "John", "347"));
        assertEqual("Check Flight Seat Number", wc().queryFlight(xid, "347"), 229);
    }
}
