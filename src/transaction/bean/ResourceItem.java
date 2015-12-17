/*
 * Created on 2005-5-17
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package transaction.bean;

import transaction.exception.InvalidIndexException;

import java.io.Serializable;

/**
 * @author RAdmin
 *         <p/>
 *         TODO To change the template for this generated type comment go to Window -
 *         Preferences - Java - Code Style - Code Templates
 */
public abstract class ResourceItem<Key> implements Cloneable, Serializable {
    protected boolean isDeleted = false;

    public abstract String[] getColumnNames();

    public abstract String[] getColumnValues();

    public abstract Key getKey();

    public abstract ResourceItem<Key> clone();

    protected abstract Object indexValue();

    public final Object getIndex(String indexName) throws InvalidIndexException {
        if (getColumnNames()[0].equals(indexName)) {
            return indexValue();
        } else {
            throw new InvalidIndexException(indexName);
        }
    }

    public final boolean isDeleted() {
        return isDeleted;
    }

    public final void delete() {
        this.isDeleted = true;
    }
}