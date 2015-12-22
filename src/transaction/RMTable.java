/*
 * Created on 2005-5-18
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package transaction;

import lockmgr.DeadlockException;
import lockmgr.LockManager;
import lockmgr.LockType;
import transaction.bean.ResourceItem;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

public class RMTable<K> implements Serializable {
    protected Hashtable<K, ResourceItem<K>> table = new Hashtable<K, ResourceItem<K>>();

    transient protected RMTable<K> parent;

    protected Hashtable<K, LockType> locks = new Hashtable<K, LockType>();

    transient protected LockManager lm;

    protected String tableName;

    protected long xid;

    public RMTable(String tableName, RMTable<K> parent, long xid, LockManager lm) {
        this.xid = xid;
        this.tableName = tableName;
        this.parent = parent;
        this.lm = lm;
    }

    public void setLockManager(LockManager lm) {
        this.lm = lm;
    }

    public void setParent(RMTable<K> parent) {
        this.parent = parent;
    }

    public String getTableName() {
        return tableName;
    }

    public void relockAll() throws DeadlockException {
        for (Object o : locks.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            if (!lm.lock(xid, tableName + ":" + entry.getKey().toString(), (LockType) entry.getValue()))
                throw new RuntimeException();
        }
    }

    public void lock(K key, LockType lockType) throws DeadlockException {
        if (!lm.lock(xid, tableName + ":" + key.toString(), lockType))
            throw new RuntimeException();
        locks.put(key, lockType);
    }

    public ResourceItem<K> get(K key) {
        ResourceItem<K> item = table.get(key);
        if (item == null && parent != null) {
            item = parent.get(key);
        }
        return item;
    }

    public void put(ResourceItem<K> item) {
        table.put(item.getKey(), item);
    }

    public void remove(ResourceItem<K> item) {
        table.remove(item.getKey());
    }

    public Set<K> keySet() {
        HashSet<K> result = new HashSet<K>(table.keySet());
        if (parent != null) {
            result.addAll(parent.table.keySet());
        }
        return result;
    }
}