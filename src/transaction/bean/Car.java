package transaction.bean;

/**
 * Created by Dawnwords on 2015/12/17.
 */
public class Car extends ResourceItem<String> {

    private String location;
    private int price;
    private int numCars, numAvail;

    public Car(String location, int price, int numCars, int numAvail) {
        this.location = location;
        this.price = price;
        this.numCars = numCars;
        this.numAvail = numAvail;
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
    protected Object indexValue() {
        return location;
    }

    @Override
    public String getKey() {
        return location;
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
    public Car clone() {
        Car car = new Car(location, price, numCars, numAvail);
        car.isDeleted = isDeleted;
        return car;
    }
}
