package transaction;

import lockmgr.DeadlockException;
import transaction.bean.*;
import transaction.exception.InvalidTransactionException;
import transaction.exception.TransactionAbortedException;

import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Workflow Controller for the Distributed Travel Reservation System.
 * <p/>
 * Description: toy implementation of the WC.  In the real
 * implementation, the WC should forward calls to either RM or TM,
 * instead of doing the things itself.
 */

public class WorkflowControllerImpl extends Host implements WorkflowController {

    private AtomicLong xidCounter;

    private RMTMDaemon daemon;

    public WorkflowControllerImpl() throws RemoteException {
        super(HostName.WC);
        xidCounter = new AtomicLong(System.currentTimeMillis());

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
    public long start() throws RemoteException {
        return xidCounter.getAndIncrement();
    }

    @Override
    public boolean commit(long xid) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        System.out.println("Committing");
        return true;
    }

    @Override
    public void abort(long xid) throws RemoteException, InvalidTransactionException {
        System.out.println("Aborting");
    }


    // ADMINISTRATIVE INTERFACE
    @Override
    public boolean addFlight(long xid, String flightNum, int numSeats, int price) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        if (flightNum == null || numSeats < 0 || price < 0) {
            return false;
        }
        RMManagerFlights flightRM = (RMManagerFlights) daemon.rm(HostName.RMFlights);
        try {
            if (flightRM.query(xid, flightNum) != null) {
                return false;
            }
            flightRM.insert(xid, new Flight(flightNum, price, numSeats, numSeats));
            return true;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    @Override
    public boolean deleteFlight(long xid, String flightNum) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        if (flightNum == null) {
            return false;
        }
        RMManagerFlights flightRM = (RMManagerFlights) daemon.rm(HostName.RMFlights);
        RMManagerReservations reservationRM = (RMManagerReservations) daemon.rm(HostName.RMReservations);
        try {
            for (ResourceItem<ReservationKey> reservation : reservationRM.query(xid)) {
                ReservationKey key = reservation.getKey();
                if (key != null && key.resvType() == ReservationType.FLIGHT && flightNum.equals(key.resvKey())) {
                    return false;
                }
            }
            return flightRM.delete(xid, flightNum);
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    @Override
    public boolean addRooms(long xid, String location, int numRooms, int price) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        if (location == null || numRooms < 0 || price < 0) {
            return false;
        }
        RMManagerHotels roomRM = (RMManagerHotels) daemon.rm(HostName.RMRooms);
        try {
            if (roomRM.query(xid, location) != null) {
                return false;
            }
            roomRM.insert(xid, new Hotel(location, price, numRooms, numRooms));
            return true;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    @Override
    public boolean deleteRooms(long xid, String location, int numRooms) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        if (location == null || numRooms < 0) {
            return false;
        }
        RMManagerHotels roomRM = (RMManagerHotels) daemon.rm(HostName.RMRooms);
        try {
            Hotel hotel = (Hotel) roomRM.query(xid, location);
            return !(hotel == null || hotel.numAvail() < numRooms) &&
                    roomRM.update(xid, location, new Hotel(hotel.location(), hotel.price(), hotel.numRooms(), hotel.numAvail() - numRooms));
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    @Override
    public boolean addCars(long xid, String location, int numCars, int price) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        if (location == null || numCars < 0 || price < 0) {
            return false;
        }
        RMManagerCars carRM = (RMManagerCars) daemon.rm(HostName.RMCars);
        try {
            if (carRM.query(xid, location) != null) {
                return false;
            }
            carRM.insert(xid, new Car(location, price, numCars, numCars));
            return true;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    @Override
    public boolean deleteCars(long xid, String location, int numCars) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        if (location == null || numCars < 0) {
            return false;
        }
        RMManagerCars carRM = (RMManagerCars) daemon.rm(HostName.RMCars);
        try {
            Car car = (Car) carRM.query(xid, location);
            return !(car == null || car.numAvail() < numCars) &&
                    carRM.update(xid, location, new Car(car.location(), car.price(), car.numCars(), car.numAvail() - numCars));
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    @Override
    public boolean newCustomer(long xid, String custName) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        if (custName == null) {
            return false;
        }
        RMManagerCustomers customerRM = (RMManagerCustomers) daemon.rm(HostName.RMCustomers);
        try {
            return customerRM.query(xid, custName) == null && customerRM.insert(xid, new Customer(custName));
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    @Override
    public boolean deleteCustomer(long xid, String custName) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        if (custName == null) {
            return false;
        }
        RMManagerCustomers customerRM = (RMManagerCustomers) daemon.rm(HostName.RMCustomers);
        RMManagerReservations reservationRM = (RMManagerReservations) daemon.rm(HostName.RMReservations);
        try {
            return customerRM.query(xid, custName) != null &&
                    customerRM.delete(xid, custName) &&
                    reservationRM.delete(xid, "custName", custName) >= 0;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    // QUERY INTERFACE
    @Override
    public int queryFlight(long xid, String flightNum) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        Flight flight = getFlight(xid, flightNum);
        return flight == null ? -1 : flight.numAvail();
    }

    @Override
    public int queryFlightPrice(long xid, String flightNum) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        Flight flight = getFlight(xid, flightNum);
        return flight == null ? -1 : flight.price();
    }

    private Flight getFlight(long xid, String flightNum) throws InvalidTransactionException, RemoteException, TransactionAbortedException {
        if (flightNum == null) {
            return null;
        }
        RMManagerFlights flightsRM = (RMManagerFlights) daemon.rm(HostName.RMFlights);
        try {
            return (Flight) flightsRM.query(xid, flightNum);
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    @Override
    public int queryRooms(long xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        Hotel hotel = getRoom(xid, location);
        return hotel == null ? -1 : hotel.numAvail();
    }

    @Override
    public int queryRoomsPrice(long xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        Hotel hotel = getRoom(xid, location);
        return hotel == null ? -1 : hotel.price();
    }

    private Hotel getRoom(long xid, String location) throws InvalidTransactionException, RemoteException, TransactionAbortedException {
        if (location == null) {
            return null;
        }
        RMManagerHotels roomRM = (RMManagerHotels) daemon.rm(HostName.RMRooms);
        try {
            return (Hotel) roomRM.query(xid, location);
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    @Override
    public int queryCars(long xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        Car car = getCar(xid, location);
        return car == null ? -1 : car.numAvail();
    }

    @Override
    public int queryCarsPrice(long xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        Car car = getCar(xid, location);
        return car == null ? -1 : car.price();
    }

    private Car getCar(long xid, String location) throws InvalidTransactionException, RemoteException, TransactionAbortedException {
        if (location == null) {
            return null;
        }
        RMManagerCars carRM = (RMManagerCars) daemon.rm(HostName.RMCars);
        try {
            return (Car) carRM.query(xid, location);
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    @Override
    public int queryCustomerBill(long xid, String custName) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        if (custName == null) {
            return -1;
        }
        RMManagerReservations reservationRM = (RMManagerReservations) daemon.rm(HostName.RMReservations);
        RMManagerCars carRM = (RMManagerCars) daemon.rm(HostName.RMCars);
        RMManagerHotels hotelRM = (RMManagerHotels) daemon.rm(HostName.RMRooms);
        RMManagerFlights flightRM = (RMManagerFlights) daemon.rm(HostName.RMFlights);
        try {
            List<ResourceItem<ReservationKey>> reservations = reservationRM.query(xid, "custName", custName);
            if (reservations == null || reservations.isEmpty()) {
                return -1;
            }
            int result = 0;
            for (ResourceItem<ReservationKey> reservation : reservations) {
                ReservationKey key = reservation.getKey();
                switch (key.resvType()) {
                    case FLIGHT:
                        Flight flight = (Flight) flightRM.query(xid, key.resvKey());
                        result += flight.price();
                        break;
                    case CAR:
                        Car car = (Car) carRM.query(xid, key.resvKey());
                        result += car.price();
                        break;
                    case HOTEL:
                        Hotel hotel = (Hotel) hotelRM.query(xid, key.resvKey());
                        result += hotel.price();
                        break;
                }
            }
            return result;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }


    // RESERVATION INTERFACE
    @Override
    public boolean reserveFlight(long xid, String custName, String flightNum) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        if (custName == null || flightNum == null) {
            return false;
        }
        RMManagerFlights flightRM = (RMManagerFlights) daemon.rm(HostName.RMFlights);
        RMManagerCustomers customerRM = (RMManagerCustomers) daemon.rm(HostName.RMCustomers);
        RMManagerReservations reservationRM = (RMManagerReservations) daemon.rm(HostName.RMReservations);
        try {
            Flight flight = (Flight) flightRM.query(xid, flightNum);
            return !(flight == null ||
                    customerRM.query(xid, custName) == null ||
                    reservationRM.query(xid, new ReservationKey(custName, ReservationType.FLIGHT, flightNum)) == null) &&
                    reservationRM.insert(xid, new Reservation(custName, ReservationType.FLIGHT, flightNum)) &&
                    flightRM.update(xid, flightNum, new Flight(flight.flightNum(), flight.price(), flight.numSeats(), flight.numAvail() - 1));
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    @Override
    public boolean reserveCar(long xid, String custName, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        if (custName == null || location == null) {
            return false;
        }
        RMManagerCars carRM = (RMManagerCars) daemon.rm(HostName.RMCars);
        RMManagerCustomers customerRM = (RMManagerCustomers) daemon.rm(HostName.RMCustomers);
        RMManagerReservations reservationRM = (RMManagerReservations) daemon.rm(HostName.RMReservations);
        try {
            Car car = (Car) carRM.query(xid, location);
            return !(car == null ||
                    customerRM.query(xid, custName) == null ||
                    reservationRM.query(xid, new ReservationKey(custName, ReservationType.CAR, location)) == null) &&
                    reservationRM.insert(xid, new Reservation(custName, ReservationType.CAR, location)) &&
                    carRM.update(xid, location, new Car(car.location(), car.price(), car.numCars(), car.numAvail() - 1));
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    @Override
    public boolean reserveRoom(long xid, String custName, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        if (custName == null || location == null) {
            return false;
        }
        RMManagerHotels hotelRM = (RMManagerHotels) daemon.rm(HostName.RMRooms);
        RMManagerCustomers customerRM = (RMManagerCustomers) daemon.rm(HostName.RMCustomers);
        RMManagerReservations reservationRM = (RMManagerReservations) daemon.rm(HostName.RMReservations);
        try {
            Hotel hotel = (Hotel) hotelRM.query(xid, location);
            return !(hotel == null ||
                    customerRM.query(xid, custName) == null ||
                    reservationRM.query(xid, new ReservationKey(custName, ReservationType.CAR, location)) == null) &&
                    reservationRM.insert(xid, new Reservation(custName, ReservationType.CAR, location)) &&
                    hotelRM.update(xid, location, new Hotel(hotel.location(), hotel.price(), hotel.numRooms(), hotel.numAvail() - 1));
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
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
