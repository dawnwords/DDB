/*
 * Created on 2005-5-17
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package transaction.bean;

/**
 * @author RAdmin
 *         <p/>
 *         TODO To change the template for this generated type comment go to Window -
 *         Preferences - Java - Code Style - Code Templates
 */
public class Reservation extends ResourceItem<ReservationKey> {

    private String custName;
    private ReservationType resvType;
    private String resvKey;

    public Reservation(String custName, ReservationType resvType, String resvKey) {
        this.custName = custName;
        this.resvType = resvType;
        this.resvKey = resvKey;
    }

    public String[] getColumnNames() {
        return new String[]{"custName", "resvType", "resvKey"};
    }

    public String[] getColumnValues() {
        return new String[]{custName, String.valueOf(resvType), String.valueOf(resvKey)};
    }

    @Override
    protected Object indexValue() {
        return custName;
    }

    public ReservationKey getKey() {
        return new ReservationKey(custName, resvType, resvKey);
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

    public Reservation clone() {
        Reservation o = new Reservation(custName, resvType, resvKey);
        o.isDeleted = isDeleted;
        return o;
    }

}