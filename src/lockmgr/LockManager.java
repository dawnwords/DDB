package lockmgr;

import java.util.BitSet;
import java.util.Vector;

/**
 * Implements a Lock Manager. Each Resource Manager creates one instance of this
 * class, to which all lock requests are directed.
 */
public class LockManager {
    /* Lock request type. */
    public static final int READ = 0;

    public static final int WRITE = 1;

    /* A lock request is considered deadlocked after 10 sec. */
    private static int DEADLOCK_TIMEOUT = 10000;

    private static int TABLE_SIZE = 2039;

    private static TPHashTable lockTable = new TPHashTable(TABLE_SIZE);

    private static TPHashTable stampTable = new TPHashTable(TABLE_SIZE);

    private static TPHashTable waitTable = new TPHashTable(TABLE_SIZE);

    /**
     * Each Resource Manager needs to construct one instance of the LockManager.
     */
    public LockManager() {
        super();
    }

    /**
     * Locks the data item identified by <tt>strData</tt> in mode
     * <tt>lockType</tt> on behalf of the transaction with id <tt>xid</tt>.
     * This is a blocking call; if the item is currently locked in a conflicting
     * lock mode, the requesting thread will sleep until the lock becomes
     * available or a deadlock is detected.
     *
     * @param xid      Transaction Identifier, should be non-negative.
     * @param strData  identifies the data element to be locked; should be non-null.
     * @param lockType one of LockManager.READ or LockManager.WRITE
     * @return true if operation succeeded; false if not (due to invalid
     * parameters).
     * @throws DeadlockException if deadlock is detected (using a timeout)
     */
    public boolean lock(int xid, String strData, int lockType) throws DeadlockException {
        // if any parameter is invalid, then return false
        if (xid < 0) {
            return false;
        }
        if (strData == null) {
            return false;
        }
        if ((lockType != TrxnObj.READ) && (lockType != TrxnObj.WRITE)) {
            return false;
        }

        // two objects in lock table for easy lookup.
        TrxnObj trxnObj = new TrxnObj(xid, strData, lockType);
        DataObj dataObj = new DataObj(xid, strData, lockType);

        // return true when there is no lock conflict or throw a deadlock exception.
        try {
            boolean bConflict = true;
            BitSet bConvert = new BitSet(1);

            while (bConflict) {
                synchronized (lockTable) {
                    // check if this lock request conflicts with existing locks
                    bConflict = lockConflict(dataObj, bConvert);
                    if (!bConflict) {
                        // remove the lockTime (if any) for this lock request
                        synchronized (stampTable) {
                            stampTable.remove(new TimeObj(xid));
                        }

                        // remove the entry for this transaction from waitTable (if it is there) as it has been granted its lock request
                        synchronized (waitTable) {
                            waitTable.remove(new WaitObj(xid, strData, lockType));
                        }

                        if (bConvert.get(0)) {
                            // lock conversion to carry out the lock conversion in the lock table
                            System.out.print("Converting lock...");
                            convertLockTableObj(trxnObj);
                            convertLockTableObj(dataObj);
                            System.out.println("done");
                        } else {
                            // a lock request that is not lock conversion
                            lockTable.add(trxnObj);
                            lockTable.add(dataObj);
                        }
                    }
                }
                if (bConflict) {
                    // lock conflict exists, wait
                    waitLock(dataObj);
                }
            }
        } catch (RedundantLockRequestException redundantlockrequest) {
            // ignore the redundant lock request
            return true;
        }
        return true;
    }

    /**
     * Unlocks all data items locked on behalf of the transaction with id
     * <tt>xid</tt>.
     *
     * @param xid Transaction Identifier, should be non-negative.
     * @return true if the operation succeeded, false if not.
     */
    public boolean unlockAll(int xid) {
        if (xid < 0) {
            return false;
        }

        TrxnObj trxnQueryObj = new TrxnObj(xid, "", -1); // Only used in elements() call below.
        synchronized (lockTable) {
            Vector vect = lockTable.elements(trxnQueryObj);

            TrxnObj trxnObj;
            Vector waitVector;
            WaitObj waitObj;
            int size = vect.size();

            for (int i = (size - 1); i >= 0; i--) {

                trxnObj = (TrxnObj) vect.elementAt(i);
                lockTable.remove(trxnObj);

                DataObj dataObj = new DataObj(trxnObj.getXId(), trxnObj.getDataName(), trxnObj.getLockType());
                lockTable.remove(dataObj);

                // check if there are any waiting transactions.
                synchronized (waitTable) {
                    // get all the transactions waiting on this dataObj
                    waitVector = waitTable.elements(dataObj);
                    int waitSize = waitVector.size();
                    for (int j = 0; j < waitSize; j++) {
                        waitObj = (WaitObj) waitVector.elementAt(j);
                        if (waitObj.getLockType() == LockManager.WRITE) {
                            if (j == 0) {
                                // get all other transactions which have locks on the data item just unlocked.
                                Vector vect1 = lockTable.elements(dataObj);

                                // remove interrupted thread from waitTable only if no other transaction has locked this data item
                                if (vect1.size() == 0 ||
                                        (vect1.size() == 1 && ((XObj) vect1.elementAt(0)).getXId() == waitObj.getXId())) {
                                    waitTable.remove(waitObj);
                                    try {
                                        synchronized (waitObj.getThread()) {
                                            waitObj.getThread().notify();
                                        }
                                    } catch (Exception e) {
                                        System.out.println("Exception on unlock\n" + e.getMessage());
                                    }
                                } else {
                                    // some other transaction still has a lock on the data item just unlocked.
                                    // So, WRITE lock cannot be granted.
                                    break;
                                }
                            }

                            // stop granting READ locks as soon as you find a WRITE lock request in the queue of requests
                            break;
                        } else if (waitObj.getLockType() == LockManager.READ) {
                            // remove interrupted thread from waitTable.
                            waitTable.remove(waitObj);

                            try {
                                synchronized (waitObj.getThread()) {
                                    waitObj.getThread().notify();
                                }
                            } catch (Exception e) {
                                System.out.println("Exception e\n" + e.getMessage());
                            }
                        }
                    }
                }
            }
        }

        return true;
    }

    /**
     * If the lock request is a conversion from READ lock to WRITE lock, then bitset is set
     *
     * @param dataObj
     * @param bitset
     * @return true if the lock request on dataObj conflicts with already existing locks,
     * false otherwise
     * @throws DeadlockException
     * @throws RedundantLockRequestException
     */
    private boolean lockConflict(DataObj dataObj, BitSet bitset) throws DeadlockException, RedundantLockRequestException {
        Vector vect = lockTable.elements(dataObj);
        DataObj dataObjAlready;
        int size = vect.size();

        for (int i = 0; i < size; i++) {
            dataObjAlready = (DataObj) vect.elementAt(i);
            if (dataObj.getXId() == dataObjAlready.getXId()) {
                if (dataObj.getLockType() == DataObj.READ) {
                    // ask for READ lock but already holds READ|WRITE lock => redundant
                    throw new RedundantLockRequestException(dataObj.getXId(), "Redundant READ lock request");
                } else if (dataObj.getLockType() == DataObj.WRITE) {
                    if (dataObjAlready.getLockType() == DataObj.WRITE) {
                        // ask for WRITE lock but already holds WRITE lock => redundant
                        throw new RedundantLockRequestException(dataObj.getXId(), "Redundant WRITE lock request");
                    } else {
                        // ask for WRITE lock but already holds READ lock => upgrade
                        System.out.println("Want WRITE, have READ, requesting lock upgrade");
                        bitset.set(0);
                        // continue iteration to check lock status of other transaction
                    }
                }
            } else {
                if (dataObj.getLockType() == DataObj.READ) {
                    if (dataObjAlready.getLockType() == DataObj.WRITE) {
                        System.out.println("Want READ, someone has WRITE");
                        return true;
                    }
                } else if (dataObj.getLockType() == DataObj.WRITE) {
                    System.out.println("Want WRITE, someone has READ or WRITE");
                    return true;
                }
            }
        }

        // no conflicting lock found, return false
        return false;

    }

    private void waitLock(DataObj dataObj) throws DeadlockException {
        // Check lockTime or add a new one.
        // Will always add new lockTime for each new lock request since
        // the timeObj is deleted each time the transaction succeeds in
        // getting a lock (see Lock() )

        TimeObj timeObj = new TimeObj(dataObj.getXId());
        TimeObj timestamp = null;
        long timeBlocked = 0;
        Thread thisThread = Thread.currentThread();
        WaitObj waitObj = new WaitObj(dataObj.getXId(), dataObj.getDataName(), dataObj.getLockType(), thisThread);

        synchronized (stampTable) {
            Vector vect = stampTable.elements(timeObj);
            if (vect.size() == 0) {
                // add the time stamp for this lock request to stampTable
                stampTable.add(timeObj);
                timestamp = timeObj;
            } else if (vect.size() == 1) {
                // lock operation could have timed out; check for deadlock
                TimeObj prevStamp = (TimeObj) vect.firstElement();
                timestamp = prevStamp;
                timeBlocked = timeObj.getTime() - prevStamp.getTime();
                if (timeBlocked >= LockManager.DEADLOCK_TIMEOUT) {
                    // the transaction has been waiting for a period greater
                    // than the timeout period
                    cleanupDeadlock(prevStamp, waitObj);
                }
            }
            // no more else. shouldn't be more than one time stamp per transaction
            // because a transaction at a given time the transaction can be blocked on just one lock request.
        }

        // suspend thread and wait until notified...

        synchronized (waitTable) {
            if (!waitTable.contains(waitObj)) {
                // register this transaction in the waitTable if it is not already there
                waitTable.add(waitObj);
            }
            // else lock manager already knows the transaction is waiting.
        }

        synchronized (thisThread) {
            try {
                thisThread.wait(LockManager.DEADLOCK_TIMEOUT - timeBlocked);
                TimeObj currTime = new TimeObj(dataObj.getXId());
                timeBlocked = currTime.getTime() - timestamp.getTime();
                if (timeBlocked >= LockManager.DEADLOCK_TIMEOUT) {
                    // the transaction has been waiting for a period greater
                    // than the timeout period
                    cleanupDeadlock(timestamp, waitObj);
                }
            } catch (InterruptedException e) {
                System.out.println("Thread interrupted?");
            }
        }
    }


    /**
     * cleans up stampTable and waitTable
     *
     * @param tmObj
     * @param waitObj
     * @throws DeadlockException
     */
    private void cleanupDeadlock(TimeObj tmObj, WaitObj waitObj) throws DeadlockException {
        synchronized (stampTable) {
            synchronized (waitTable) {
                stampTable.remove(tmObj);
                waitTable.remove(waitObj);
            }
        }
        throw new DeadlockException(waitObj.getXId(), "Sleep timeout...deadlock.");
    }

    private void convertLockTableObj(TrxnObj trxnObj) {
        trxnObj.setLockType(TrxnObj.READ);
        TrxnObj trxnObj2 = (TrxnObj) lockTable.get(trxnObj);
        trxnObj2.setLockType(TrxnObj.WRITE);
    }
}