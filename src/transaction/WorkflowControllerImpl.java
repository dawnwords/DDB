package transaction;

import lockmgr.DeadlockException;
import transaction.bean.*;
import transaction.exception.TransactionAbortedException;
import transaction.exception.UnaccessibleException;
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
    private Hashtable<HostName, Remote> hostMap;

    public WorkflowControllerImpl() throws RemoteException {
        super(HostName.WC);
        xidCounter = new AtomicLong(System.currentTimeMillis());
        hostMap = new Hashtable<HostName, Remote>();
    }

    public static void main(String args[]) {
        try {
            new WorkflowControllerImpl().startUp();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void startUp() {
        bindRMIRegistry();
        bind(HostName.RMCars);
        bind(HostName.RMFlights);
        bind(HostName.RMRooms);
        bind(HostName.RMCustomers);
        bind(HostName.RMReservations);
        bind(HostName.TM);
    }

    private boolean bind(HostName who) {
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

    private <K> ResourceManager<K> rm(HostName who) {
        return (ResourceManager<K>) hostMap.get(who);
    }

    private TransactionManager tm() {
        return (TransactionManager) hostMap.get(HostName.TM);
    }

    // TRANSACTION INTERFACE
    @Override
    public long start() throws RemoteException {
        long xid = xidCounter.getAndIncrement();
        Log.i("Start Transaction: %d", xid);
        tm().start(xid);
        return xid;
    }

    @Override
    public boolean commit(long xid) throws RemoteException, TransactionAbortedException {
        Log.i("Commit Transaction: %d", xid);
        return tm().commit(xid);
    }

    @Override
    public void abort(long xid) throws RemoteException {
        Log.i("Abort Transaction: %d", xid);
        tm().abort(xid);
    }

    // ADMINISTRATIVE INTERFACE
    private <K> boolean exist(ResourceManager<K> rm, long xid, K key) throws DeadlockException, RemoteException {
        return rm.query(xid, key) != null;
    }

    private void abortForRMException(long xid, Exception e) throws RemoteException, TransactionAbortedException {
        if (e.getCause() instanceof DeadlockException || e.getCause() instanceof UnaccessibleException) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } else if (e instanceof RemoteException) {
            throw (RemoteException) e;
        } else {
            throw new RuntimeException("Cannot reach here");
        }
    }

    private <T extends ResourceItem<String>> boolean add(long xid, String key, int num, int price, Class<T> clazz, HostName who) throws RemoteException, TransactionAbortedException {
        if (key == null || num < 0 || price < 0) {
            return false;
        }
        ResourceManager rm = rm(who);
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
            abortForRMException(xid, e);
        }
        return false;
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
        ResourceManager flightRM = rm(HostName.RMFlights);
        ResourceManager reservationRM = rm(HostName.RMReservations);
        try {
            List<ResourceItem<ReservationKey>> reservations = reservationRM.query(xid);
            for (ResourceItem<ReservationKey> reservation : reservations) {
                ReservationKey key = reservation.getKey();
                if (key != null && key.resvType() == ReservationType.FLIGHT && flightNum.equals(key.resvKey())) {
                    return false;
                }
            }
            return flightRM.delete(xid, flightNum);
        } catch (Exception e) {
            abortForRMException(xid, e);
        }
        return false;
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
        ResourceManager roomRM = rm(HostName.RMRooms);
        try {
            Hotel hotel = (Hotel) roomRM.query(xid, location);
            return !(hotel == null || hotel.numAvail() < numRooms) &&
                    roomRM.update(xid, location, new Hotel(hotel.location(), hotel.price(), hotel.numRooms(), hotel.numAvail() - numRooms));
        } catch (Exception e) {
            abortForRMException(xid, e);
        }
        return false;
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
        ResourceManager carRM = rm(HostName.RMCars);
        try {
            Car car = (Car) carRM.query(xid, location);
            return !(car == null || car.numAvail() < numCars) &&
                    carRM.update(xid, location, new Car(car.location(), car.price(), car.numCars(), car.numAvail() - numCars));
        } catch (Exception e) {
            abortForRMException(xid, e);
        }
        return false;
    }

    @Override
    public boolean newCustomer(long xid, String custName) throws RemoteException, TransactionAbortedException {
        if (custName == null) {
            return false;
        }
        ResourceManager customerRM = rm(HostName.RMCustomers);
        try {
            return !exist(customerRM, xid, custName) && customerRM.insert(xid, new Customer(custName));
        } catch (Exception e) {
            abortForRMException(xid, e);
        }
        return false;
    }

    @Override
    public boolean deleteCustomer(long xid, String custName) throws RemoteException, TransactionAbortedException {
        if (custName == null) {
            return false;
        }
        ResourceManager customerRM = rm(HostName.RMCustomers);
        ResourceManager reservationRM = rm(HostName.RMReservations);
        try {
            return exist(customerRM, xid, custName) &&
                    customerRM.delete(xid, custName) &&
                    reservationRM.delete(xid, "custName", custName) >= 0;
        } catch (Exception e) {
            abortForRMException(xid, e);
        }
        return false;
    }

    // QUERY INTERFACE
    private <T> T get(long xid, String key, Class<? extends T> clazz, HostName who) throws RemoteException {
        if (key == null) {
            return null;
        }
        ResourceManager rm = rm(who);
        try {
            return clazz.cast(rm.query(xid, key));
        } catch (DeadlockException e) {
            throw new RemoteException(String.format("Deadlock detected for %d:%s", xid, e));
        }
    }

    @Override
    public int queryFlight(long xid, String flightNum) throws RemoteException {
        Flight flight = get(xid, flightNum, Flight.class, HostName.RMFlights);
        return flight == null ? -1 : flight.numAvail();
    }

    @Override
    public int queryFlightPrice(long xid, String flightNum) throws RemoteException {
        Flight flight = get(xid, flightNum, Flight.class, HostName.RMFlights);
        return flight == null ? -1 : flight.price();
    }

    @Override
    public int queryRooms(long xid, String location) throws RemoteException {
        Hotel hotel = get(xid, location, Hotel.class, HostName.RMRooms);
        return hotel == null ? -1 : hotel.numAvail();
    }

    @Override
    public int queryRoomsPrice(long xid, String location) throws RemoteException {
        Hotel hotel = get(xid, location, Hotel.class, HostName.RMRooms);
        return hotel == null ? -1 : hotel.price();
    }

    @Override
    public int queryCars(long xid, String location) throws RemoteException {
        Car car = get(xid, location, Car.class, HostName.RMCars);
        return car == null ? -1 : car.numAvail();
    }

    @Override
    public int queryCarsPrice(long xid, String location) throws RemoteException {
        Car car = get(xid, location, Car.class, HostName.RMCars);
        return car == null ? -1 : car.price();
    }

    @Override
    public int queryCustomerBill(long xid, String custName) throws RemoteException {
        if (custName == null) {
            return -1;
        }
        ResourceManager reservationRM = rm(HostName.RMReservations);
        ResourceManager carRM = rm(HostName.RMCars);
        ResourceManager hotelRM = rm(HostName.RMRooms);
        ResourceManager flightRM = rm(HostName.RMFlights);
        ResourceManager customerRM = rm(HostName.RMCustomers);
        try {
            if(customerRM.query(xid, custName) == null) {
                return -1;
            }
            List<ResourceItem<ReservationKey>> reservations = reservationRM.query(xid, "custName", custName);
            if (reservations == null) {
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
            throw new RemoteException(String.format("Deadlock detected for %d:%s", xid, e));
        }
    }


    // RESERVATION INTERFACE
    private <T extends ResourceItem<String>> boolean reserve(long xid, String custName, String key, Class<T> clazz, ReservationType type, HostName who) throws RemoteException, TransactionAbortedException {
        if (custName == null || key == null) {
            return false;
        }
        ResourceManager rm = rm(who);
        ResourceManager customerRM = rm(HostName.RMCustomers);
        ResourceManager reservationRM = rm(HostName.RMReservations);
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
        } catch (Exception e) {
            abortForRMException(xid, e);
        }
        return false;
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
        dieTime = DieTime.NO_DIE;
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

    @Override
    public boolean dieNow(HostName who) throws RemoteException {
        if (who == HostName.ALL) {
            boolean result = true;
            for (Remote host : hostMap.values()) {
                result = result && dieNow(host);
            }
            return result;
        }
        Remote host = hostMap.get(who);
        if (host == null) {
            throw new IllegalArgumentException("no such host:" + who);
        }
        return dieNow(host);
    }

    public boolean dieRMAfterEnlist(HostName who) throws RemoteException {
        return dieRM(who, DieTime.AFTER_ENLIST);
    }

    public boolean dieRMBeforePrepare(HostName who) throws RemoteException {
        return dieRM(who, DieTime.BEFORE_PREPARE);
    }

    public boolean dieRMAfterPrepare(HostName who) throws RemoteException {
        return dieRM(who, DieTime.AFTER_PREPARE);
    }

    public boolean dieRMBeforeCommit(HostName who) throws RemoteException {
        return dieRM(who, DieTime.BEFORE_COMMIT);
    }

    public boolean dieRMBeforeAbort(HostName who) throws RemoteException {
        return dieRM(who, DieTime.BEFORE_ABORT);
    }

    public boolean dieTMBeforeCommit() throws RemoteException {
        return dieTM(DieTime.BEFORE_COMMIT);
    }

    public boolean dieTMAfterCommit() throws RemoteException {
        return dieTM(DieTime.AFTER_COMMIT);
    }

    @Override
    public boolean dieNow() throws RemoteException {
        throw new RuntimeException("WC will not die!");
    }

    private boolean dieNow(Remote host) {
        try {
            host.dieNow();
        } catch (RemoteException ignored) {
        }
        return true;
    }

    private boolean dieRM(HostName who, DieTime time) throws RemoteException {
        switch (who) {
            case RMFlights:
            case RMRooms:
            case RMReservations:
            case RMCars:
                hostMap.get(who).setDieTime(time);
                return true;
            default:
                return false;
        }
    }

    private boolean dieTM(DieTime time) throws RemoteException {
        hostMap.get(HostName.TM).setDieTime(time);
        return true;
    }
}
