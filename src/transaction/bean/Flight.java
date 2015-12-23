package transaction.bean;

/**
 * Created by Dawnwords on 2015/12/17.
 */
public class Flight extends ResourceItem<String> {

    private String flightNum;
    private int price;
    private int numSeats;
    private int numAvail;

    public Flight(String flightNum, int price, int numSeats, int numAvail) {
        this.flightNum = flightNum;
        this.price = price;
        this.numSeats = numSeats;
        this.numAvail = numAvail;
    }

    public String flightNum() {
        return flightNum;
    }

    public int price() {
        return price;
    }

    public int numSeats() {
        return numSeats;
    }

    public int numAvail() {
        return numAvail;
    }

    public void decreaseAvail() {
        numAvail--;
    }

    @Override
    public String[] getColumnNames() {
        return new String[]{"flightNum", "price", "numSeats", "numAvail"};
    }

    @Override
    public String[] getColumnValues() {
        return new String[]{flightNum, String.valueOf(price), String.valueOf(numSeats), String.valueOf(numAvail)};
    }

    @Override
    public String getKey() {
        return flightNum;
    }

    @Override
    public ResourceItem<String> clone() {
        Flight flight = new Flight(flightNum, price, numSeats, numAvail);
        flight.isDeleted = isDeleted;
        return flight;
    }

    @Override
    protected Object indexValue() {
        return flightNum;
    }

    @Override
    public String toString() {
        return "Flight{" +
                "flightNum='" + flightNum + '\'' +
                ", price=" + price +
                ", numSeats=" + numSeats +
                ", numAvail=" + numAvail +
                '}';
    }
}
