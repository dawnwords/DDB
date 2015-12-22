package transaction.bean;

/**
 * Created by Dawnwords on 2015/12/17.
 */
public class Hotel extends ResourceItem<String> {
    private String location;
    private int price;
    private int numRooms;
    private int numAvail;

    public Hotel(String location, int price, int numRooms, int numAvail) {
        this.location = location;
        this.price = price;
        this.numRooms = numRooms;
        this.numAvail = numAvail;
    }

    public String location() {
        return location;
    }

    public int price() {
        return price;
    }

    public int numRooms() {
        return numRooms;
    }

    public int numAvail() {
        return numAvail;
    }

    public void decreaseAvail() {
        numAvail--;
    }

    @Override
    public String[] getColumnNames() {
        return new String[]{"location", "price", "numSeats", "numAvail"};
    }

    @Override
    public String[] getColumnValues() {
        return new String[]{location, String.valueOf(price), String.valueOf(numRooms), String.valueOf(numAvail)};
    }

    @Override
    public String getKey() {
        return location;
    }

    @Override
    public ResourceItem<String> clone() {
        Hotel hotel = new Hotel(location, price, numRooms, numAvail);
        hotel.isDeleted = isDeleted;
        return hotel;
    }

    @Override
    protected Object indexValue() {
        return location;
    }
}
