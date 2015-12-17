package transaction.bean;

/**
 * Created by Dawnwords on 2015/12/17.
 */
public class Flight extends ResourceItem<String> {

    private int flightNum;
    private double price;
    private int numRooms;
    private int numAvail;

    public Flight(int flightNum, double price, int numRooms, int numAvail) {
        this.flightNum = flightNum;
        this.price = price;
        this.numRooms = numRooms;
        this.numAvail = numAvail;
    }

    public int flightNum() {
        return flightNum;
    }

    public double price() {
        return price;
    }

    public int numRooms() {
        return numRooms;
    }

    public int numAvail() {
        return numAvail;
    }

    @Override
    public String[] getColumnNames() {
        return new String[]{"flightNum", "price", "numRooms", "numAvail"};
    }

    @Override
    public String[] getColumnValues() {
        return new String[]{String.valueOf(flightNum), String.valueOf(price), String.valueOf(numRooms), String.valueOf(numAvail)};
    }

    @Override
    public String getKey() {
        return String.valueOf(flightNum);
    }

    @Override
    public ResourceItem<String> clone() {
        Flight flight = new Flight(flightNum, price, numRooms, numAvail);
        flight.isDeleted = isDeleted;
        return flight;
    }

    @Override
    protected Object indexValue() {
        return flightNum;
    }
}
