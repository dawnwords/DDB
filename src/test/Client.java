package test;

/**
 * A toy client of the Distributed Travel Reservation System.
 */

public class Client extends BaseClient {

    public static void main(String args[]) {
        new Client().test();
    }

    @Override
    public void test() {
        try {
            long xid = wc().start();
            assertTrue("Add flight", wc().addFlight(xid, "347", 230, 999));
            assertTrue("Add room", wc().addRooms(xid, "SFO", 500, 150));
            assertEqual("Check Flight Seat Number", wc().queryFlight(xid, "347"), 230);
            assertTrue("Add customer", wc().newCustomer(xid, "John"));
            assertTrue("Reserve flight", wc().reserveFlight(xid, "John", "347"));
            assertTrue("Commit", wc().commit(xid));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
