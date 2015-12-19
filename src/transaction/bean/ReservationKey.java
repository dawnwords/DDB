/*
 * Created on 2005-5-17
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package transaction.bean;

import java.io.Serializable;

/**
 * @author RAdmin
 *         <p/>
 *         TODO To change the template for this generated type comment go to Window -
 *         Preferences - Java - Code Style - Code Templates
 */
public class ReservationKey implements Serializable {
    private String custName;

    private ReservationType resvType;

    private String resvKey;

    public ReservationKey(String custName, ReservationType resvType, String resvKey) {
        this.custName = custName;
        this.resvKey = resvKey;
        this.resvType = resvType;
    }

    public String custName() {
        return custName;
    }

    public ReservationType resvType() {
        return resvType;
    }

    public String resvKey() {
        return resvKey;
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof ReservationKey))
            return false;
        if (this == o)
            return true;
        ReservationKey k = (ReservationKey) o;
        return k.custName.equals(custName) && k.resvKey.equals(resvKey) && k.resvType == resvType;
    }

    @Override
    public int hashCode() {
        int result = custName != null ? custName.hashCode() : 0;
        result = 31 * result + (resvType != null ? resvType.hashCode() : 0);
        result = 31 * result + (resvKey != null ? resvKey.hashCode() : 0);
        return result;
    }

    public String toString() {
        return "[customer name=" + custName + ";" + "resvKey=" + resvKey + ";" + "resvType=" + resvType + "]";
    }
}