package transaction;

import lockmgr.DeadlockException;
import transaction.bean.*;
import transaction.exception.TransactionAbortedException;
import util.Log;

import java.rmi.RemoteException;
import java.util.Hashtable;
import java.util.List;
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
    }

    public static void main(String args[]) {
        try {
            new WorkflowControllerImpl().startUp();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void startUp() {
        daemon.start();
        bindRMIRegistry();
    }

    // TRANSACTION INTERFACE
    @Override
    public long start() throws RemoteException {
        long xid = xidCounter.getAndIncrement();
        Log.i("Start Transaction: %d", xid);
        daemon.tm().start(xid);
        return xid;
    }

    @Override
    public boolean commit(long xid) throws RemoteException, TransactionAbortedException {
        Log.i("Commit Transaction: %d", xid);
        daemon.tm().commit(xid);
        return true;
    }

    @Override
    public void abort(long xid) throws RemoteException {
        Log.i("Abort Transaction: %d", xid);
        daemon.tm().abort(xid);
    }


    // ADMINISTRATIVE INTERFACE
    private <K> boolean exist(ResourceManager<K> rm, long xid, K key) throws DeadlockException, RemoteException {
        return rm.query(xid, key) != null;
    }

    private <T extends ResourceItem<String>> boolean add(long xid, String key, int num, int price, Class<T> clazz, HostName who) throws RemoteException, TransactionAbortedException {
        if (key == null || num < 0 || price < 0) {
            return false;
        }
        ResourceManager rm = daemon.rm(who);
        try {
            if (exist(rm, xid, key)) {
                return false;
            }
            T newItem;
            try {
                newItem = clazz.getConstructor(String.class, int.class, int.class, int.class).newInstance(key, price, num, num);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return rm.insert(xid, newItem);
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    @Override
    public boolean addFlight(long xid, String flightNum, int numSeats, int price) throws RemoteException, TransactionAbortedException {
        return add(xid, flightNum, numSeats, price, Flight.class, HostName.RMFlights);
    }

    @Override
    public boolean deleteFlight(long xid, String flightNum) throws RemoteException, TransactionAbortedException {
        if (flightNum == null) {
            return false;
        }
        ResourceManager flightRM = daemon.rm(HostName.RMFlights);
        ResourceManager reservationRM = daemon.rm(HostName.RMReservations);
        try {
            List<ResourceItem<ReservationKey>> reservations = reservationRM.query(xid);
            for (ResourceItem<ReservationKey> reservation : reservations) {
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
    public boolean addRooms(long xid, String location, int numRooms, int price) throws RemoteException, TransactionAbortedException {
        return add(xid, location, numRooms, price, Hotel.class, HostName.RMRooms);
    }

    @Override
    public boolean deleteRooms(long xid, String location, int numRooms) throws RemoteException, TransactionAbortedException {
        if (location == null || numRooms < 0) {
            return false;
        }
        ResourceManager roomRM = daemon.rm(HostName.RMRooms);
        try {
            Hotel hotel = (Hotel) roomRM.query(xid, location);
            return !(hotel == null || hotel.numAvail() < numRooms) &&
                    roomRM.update(xid, location, new Hotel(hotel.location(), hotel.price(), hotel.numRooms(), hotel.numAvail() - numRooms));
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    @Override
    public boolean addCars(long xid, String location, int numCars, int price) throws RemoteException, TransactionAbortedException {
        return add(xid, location, numCars, price, Car.class, HostName.RMCars);
    }

    @Override
    public boolean deleteCars(long xid, String location, int numCars) throws RemoteException, TransactionAbortedException {
        if (location == null || numCars < 0) {
            return false;
        }
        ResourceManager carRM = daemon.rm(HostName.RMCars);
        try {
            Car car = (Car) carRM.query(xid, location);
            return !(car == null || car.numAvail() < numCars) &&
                    carRM.update(xid, location, new Car(car.location(), car.price(), car.numCars(), car.numAvail() - numCars));
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    @Override
    public boolean newCustomer(long xid, String custName) throws RemoteException, TransactionAbortedException {
        if (custName == null) {
            return false;
        }
        ResourceManager customerRM = daemon.rm(HostName.RMCustomers);
        try {
            return !exist(customerRM, xid, custName) && customerRM.insert(xid, new Customer(custName));
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    @Override
    public boolean deleteCustomer(long xid, String custName) throws RemoteException, TransactionAbortedException {
        if (custName == null) {
            return false;
        }
        ResourceManager customerRM = daemon.rm(HostName.RMCustomers);
        ResourceManager reservationRM = daemon.rm(HostName.RMReservations);
        try {
            return exist(customerRM, xid, custName) &&
                    customerRM.delete(xid, custName) &&
                    reservationRM.delete(xid, "custName", custName) >= 0;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    // QUERY INTERFACE
    private <T> T get(long xid, String key, Class<? extends T> clazz, HostName who) throws RemoteException, TransactionAbortedException {
        if (key == null) {
            return null;
        }
        ResourceManager rm = daemon.rm(who);
        try {
            return clazz.cast(rm.query(xid, key));
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    @Override
    public int queryFlight(long xid, String flightNum) throws RemoteException, TransactionAbortedException {
        Flight flight = get(xid, flightNum, Flight.class, HostName.RMFlights);
        return flight == null ? -1 : flight.numAvail();
    }

    @Override
    public int queryFlightPrice(long xid, String flightNum) throws RemoteException, TransactionAbortedException {
        Flight flight = get(xid, flightNum, Flight.class, HostName.RMFlights);
        return flight == null ? -1 : flight.price();
    }

    @Override
    public int queryRooms(long xid, String location) throws RemoteException, TransactionAbortedException {
        Hotel hotel = get(xid, location, Hotel.class, HostName.RMRooms);
        return hotel == null ? -1 : hotel.numAvail();
    }

    @Override
    public int queryRoomsPrice(long xid, String location) throws RemoteException, TransactionAbortedException {
        Hotel hotel = get(xid, location, Hotel.class, HostName.RMRooms);
        return hotel == null ? -1 : hotel.price();
    }

    @Override
    public int queryCars(long xid, String location) throws RemoteException, TransactionAbortedException {
        Car car = get(xid, location, Car.class, HostName.RMCars);
        return car == null ? -1 : car.numAvail();
    }

    @Override
    public int queryCarsPrice(long xid, String location) throws RemoteException, TransactionAbortedException {
        Car car = get(xid, location, Car.class, HostName.RMCars);
        return car == null ? -1 : car.price();
    }

    @Override
    public int queryCustomerBill(long xid, String custName) throws RemoteException, TransactionAbortedException {
        if (custName == null) {
            return -1;
        }
        ResourceManager reservationRM = daemon.rm(HostName.RMReservations);
        ResourceManager carRM = daemon.rm(HostName.RMCars);
        ResourceManager hotelRM = daemon.rm(HostName.RMRooms);
        ResourceManager flightRM = daemon.rm(HostName.RMFlights);
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
    private <T extends ResourceItem<String>> boolean reserve(long xid, String custName, String key, Class<T> clazz, ReservationType type, HostName who) throws RemoteException, TransactionAbortedException {
        if (custName == null || key == null) {
            return false;
        }
        ResourceManager rm = daemon.rm(who);
        ResourceManager customerRM = daemon.rm(HostName.RMCustomers);
        ResourceManager reservationRM = daemon.rm(HostName.RMReservations);
        try {
            ResourceItem<String> oldItem = rm.query(xid, key);
            if (oldItem == null ||
                    !exist(customerRM, xid, custName) ||
                    exist(reservationRM, xid, new ReservationKey(custName, type, key)) ||
                    !reservationRM.insert(xid, new Reservation(custName, type, key))) {
                return false;
            }
            ResourceItem<String> newItem = oldItem.clone();
            try {
                clazz.getMethod("decreaseAvail").invoke(newItem);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return rm.update(xid, key, newItem);
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "Deadlock detected:" + e);
        }
    }

    @Override
    public boolean reserveFlight(long xid, String custName, String flightNum) throws RemoteException, TransactionAbortedException {
        return reserve(xid, custName, flightNum, Flight.class, ReservationType.FLIGHT, HostName.RMFlights);
    }

    @Override
    public boolean reserveCar(long xid, String custName, String location) throws RemoteException, TransactionAbortedException {
        return reserve(xid, custName, location, Car.class, ReservationType.CAR, HostName.RMCars);
    }

    @Override
    public boolean reserveRoom(long xid, String custName, String location) throws RemoteException, TransactionAbortedException {
        return reserve(xid, custName, location, Hotel.class, ReservationType.HOTEL, HostName.RMRooms);
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
        private Hashtable<HostName, Remote> hostMap;

        public RMTMDaemon() {
            hostMap = new Hashtable<HostName, Remote>();
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
                    !bind(HostName.RMCustomers) ||
                    !bind(HostName.RMReservations) ||
                    !bind(HostName.TM)) {
                return false;
            }
            try {
                boolean result = true;
                for (Remote host : hostMap.values()) {
                    result = result && host.reconnect();
                }
                return result;
            } catch (Exception e) {
                System.err.println("Some RM cannot reconnect:" + e);
                return false;
            }
        }

        boolean bind(HostName who) {
            Remote host = hostMap.get(who);
            if (host != null) {
                try {
                    host.ping();
                    return true;
                } catch (Exception ignored) {
                }
            }

            try {
                hostMap.put(who, (Remote) lookUp(who));
                Log.i("WC bound to " + who.name());
                return true;
            } catch (Exception e) {
                Log.e("WC cannot bind to " + who.name());
                e.printStackTrace();
                return false;
            }
        }

        boolean dieNow(HostName name) {
            if (name == HostName.ALL) {
                boolean result = true;
                for (Remote host : hostMap.values()) {
                    result = result && dieNow(host);
                }
                return result;
            }
            Remote host = hostMap.get(name);
            if (host == null) {
                throw new IllegalArgumentException("no such host:" + name);
            }
            return dieNow(host);
        }

        boolean dieNow(Remote host) {
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

        <K> ResourceManager<K> rm(HostName who) {
            return (ResourceManager<K>) hostMap.get(who);
        }

        TransactionManager tm() {
            return (TransactionManager) hostMap.get(HostName.TM);
        }
    }
}
