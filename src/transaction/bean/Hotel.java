package transaction.bean;

/**
 * Created by Dawnwords on 2015/12/17.
 */
public class Hotel extends ResourceItem<String> {
    private String location;
    private int price;
    private int numCars;
    private int numAvail;

    public Hotel(String location, int price, int numCars, int numAvail) {
        this.location = location;
        this.price = price;
        this.numCars = numCars;
        this.numAvail = numAvail;
    }

    public String location() {
        return location;
    }

    public int price() {
        return price;
    }

    public int numCars() {
        return numCars;
    }

    public int numAvail() {
        return numAvail;
    }

    @Override
    public String[] getColumnNames() {
        return new String[]{"location", "price", "numCars", "numAvail"};
    }

    @Override
    public String[] getColumnValues() {
        return new String[]{location, String.valueOf(price), String.valueOf(numCars), String.valueOf(numAvail)};
    }

    @Override
    public String getKey() {
        return location;
    }

    @Override
    public ResourceItem<String> clone() {
        Hotel hotel = new Hotel(location, price, numCars, numAvail);
        hotel.isDeleted = isDeleted;
        return hotel;
    }

    @Override
    protected Object indexValue() {
        return location;
    }
}
