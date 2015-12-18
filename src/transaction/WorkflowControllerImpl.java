package transaction;

import transaction.exception.InvalidTransactionException;
import transaction.exception.TransactionAbortedException;

import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.util.Hashtable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Workflow Controller for the Distributed Travel Reservation System.
 * <p/>
 * Description: toy implementation of the WC.  In the real
 * implementation, the WC should forward calls to either RM or TM,
 * instead of doing the things itself.
 */

public class WorkflowControllerImpl extends Host implements WorkflowController {

    private int flightCounter, flightPrice, carsCounter, carsPrice, roomsCounter, roomsPrice;
    private AtomicInteger xidCounter;

    private RMTMDaemon daemon;

    public WorkflowControllerImpl() throws RemoteException {
        super(HostName.WC);
        xidCounter = new AtomicInteger(1);

        daemon = new RMTMDaemon();
        daemon.start();
    }

    public static void main(String args[]) {
        System.setSecurityManager(new RMISecurityManager());

        String rmiPort = System.getProperty("rmiPort");
        if (rmiPort == null) {
            rmiPort = "";
        } else if (!rmiPort.equals("")) {
            rmiPort = "//:" + rmiPort + "/";
        }

        try {
            WorkflowControllerImpl obj = new WorkflowControllerImpl();
            Naming.rebind(rmiPort + HostName.WC, obj);
            System.out.println("WC bound");
        } catch (Exception e) {
            System.err.println("WC not bound:" + e);
            System.exit(1);
        }
    }

    // TRANSACTION INTERFACE
    @Override
    public int start() throws RemoteException {
        return xidCounter.getAndIncrement();
    }

    @Override
    public boolean commit(int xid) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        System.out.println("Committing");
        return true;
    }

    @Override
    public void abort(int xid) throws RemoteException, InvalidTransactionException {
        System.out.println("Aborting");
    }


    // ADMINISTRATIVE INTERFACE
    @Override
    public boolean addFlight(int xid, String flightNum, int numSeats, int price) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        flightCounter += numSeats;
        flightPrice = price;
        return true;
    }

    @Override
    public boolean deleteFlight(int xid, String flightNum) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        flightCounter = 0;
        flightPrice = 0;
        return true;
    }

    @Override
    public boolean addRooms(int xid, String location, int numRooms, int price) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        roomsCounter += numRooms;
        roomsPrice = price;
        return true;
    }

    @Override
    public boolean deleteRooms(int xid, String location, int numRooms) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        roomsCounter = 0;
        roomsPrice = 0;
        return true;
    }

    @Override
    public boolean addCars(int xid, String location, int numCars, int price) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        carsCounter += numCars;
        carsPrice = price;
        return true;
    }

    @Override
    public boolean deleteCars(int xid, String location, int numCars) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        carsCounter = 0;
        carsPrice = 0;
        return true;
    }

    @Override
    public boolean newCustomer(int xid, String custName) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        return true;
    }

    @Override
    public boolean deleteCustomer(int xid, String custName) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        return true;
    }


    // QUERY INTERFACE
    @Override
    public int queryFlight(int xid, String flightNum) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        return flightCounter;
    }

    @Override
    public int queryFlightPrice(int xid, String flightNum) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        return flightPrice;
    }

    @Override
    public int queryRooms(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        return roomsCounter;
    }

    @Override
    public int queryRoomsPrice(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        return roomsPrice;
    }

    @Override
    public int queryCars(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        return carsCounter;
    }

    @Override
    public int queryCarsPrice(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        return carsPrice;
    }

    @Override
    public int queryCustomerBill(int xid, String custName) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        return 0;
    }


    // RESERVATION INTERFACE
    @Override
    public boolean reserveFlight(int xid, String custName, String flightNum) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        flightCounter--;
        return true;
    }

    @Override
    public boolean reserveCar(int xid, String custName, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        carsCounter--;
        return true;
    }

    @Override
    public boolean reserveRoom(int xid, String custName, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        roomsCounter--;
        return true;
    }

    // TECHNICAL/TESTING INTERFACE
    @Override
    public boolean reconnect() throws RemoteException {
        return daemon.reconnect();
    }

    @Override
    public boolean dieNow(HostName who) throws RemoteException {
        return daemon.dieNow(who);
    }

    public boolean dieRMAfterEnlist(HostName who) throws RemoteException {
        return daemon.dieRM(who, DieTime.AFTER_ENLIST);
    }

    public boolean dieRMBeforePrepare(HostName who) throws RemoteException {
        return daemon.dieRM(who, DieTime.BEFORE_PREPARE);
    }

    public boolean dieRMAfterPrepare(HostName who) throws RemoteException {
        return daemon.dieRM(who, DieTime.AFTER_PREPARE);
    }

    public boolean dieRMBeforeCommit(HostName who) throws RemoteException {
        return daemon.dieRM(who, DieTime.BEFORE_COMMIT);
    }

    public boolean dieRMBeforeAbort(HostName who) throws RemoteException {
        return daemon.dieRM(who, DieTime.BEFORE_ABORT);
    }

    public boolean dieTMBeforeCommit() throws RemoteException {
        return daemon.dieTM(DieTime.BEFORE_COMMIT);
    }

    public boolean dieTMAfterCommit() throws RemoteException {
        return daemon.dieTM(DieTime.AFTER_COMMIT);
    }

    private class RMTMDaemon extends Thread {
        private Hashtable<HostName, Host> hostMap;
        private String rmiPort;

        public RMTMDaemon() {
            hostMap = new Hashtable<HostName, Host>();
            rmiPort = System.getProperty("rmiPort");
            if (rmiPort == null) {
                rmiPort = "";
            } else if (!rmiPort.equals("")) {
                rmiPort = "//:" + rmiPort + "/";
            }
        }

        @Override
        public void run() {
            while (!isInterrupted()) {
                reconnect();
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ignored) {
                }
            }
        }

        boolean reconnect() {
            if (!bind(HostName.RMCars) ||
                    !bind(HostName.RMFlights) ||
                    !bind(HostName.RMRooms) ||
                    !bind(HostName.RMReservations) ||
                    !bind(HostName.TM)) {
                return false;
            }
            try {
                boolean result = true;
                for (Host host : hostMap.values()) {
                    result = result && host.reconnect();
                }
                return result;
            } catch (Exception e) {
                System.err.println("Some RM cannot reconnect:" + e);
                return false;
            }
        }

        boolean bind(HostName who) {
            Host host = hostMap.get(who);
            if (host != null) {
                try {
                    host.ping();
                    return true;
                } catch (Exception ignored) {
                }
            }

            try {
                hostMap.put(who, (Host) Naming.lookup(rmiPort + who.name()));
                System.out.println("WC bound to " + who.name());
                return true;
            } catch (Exception e) {
                System.err.println("WC cannot bind to " + who.name());
                e.printStackTrace();
                return false;
            }
        }

        boolean dieNow(HostName name) {
            if (name == HostName.ALL) {
                boolean result = true;
                for (Host host : hostMap.values()) {
                    result = result && dieNow(host);
                }
                return result;
            }
            Host host = hostMap.get(name);
            if (host == null) {
                throw new IllegalArgumentException("no such host:" + name);
            }
            return dieNow(host);
        }

        boolean dieNow(Host host) {
            try {
                host.dieNow();
            } catch (RemoteException e) {
                e.printStackTrace();
                return false;
            }
            return true;
        }

        boolean dieRM(HostName who, DieTime time) throws RemoteException {
            switch (who) {
                case RMFlights:
                case RMRooms:
                case RMReservations:
                case RMCars:
                    daemon.hostMap.get(who).setDieTime(time);
                    return true;
                default:
                    return false;
            }
        }

        boolean dieTM(DieTime time) throws RemoteException {
            daemon.hostMap.get(HostName.TM).setDieTime(time);
            return true;
        }

        ResourceManager rm(HostName who) {
            return (ResourceManager) hostMap.get(who);
        }

        TransactionManager tm() {
            return (TransactionManager) hostMap.get(HostName.TM);
        }
    }
}
