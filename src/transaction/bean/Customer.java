package transaction.bean;

/**
 * Created by Dawnwords on 2015/12/18.
 */
public class Customer extends ResourceItem<String> {
    private String custName;

    public Customer(String custName) {
        this.custName = custName;
    }

    @Override
    public String[] getColumnNames() {
        return new String[]{"custName"};
    }

    @Override
    public String[] getColumnValues() {
        return new String[]{custName};
    }

    @Override
    public String getKey() {
        return custName;
    }

    @Override
    public ResourceItem<String> clone() {
        Customer customer = new Customer(custName);
        customer.isDeleted = isDeleted;
        return customer;
    }

    @Override
    protected Object indexValue() {
        return custName;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "custName='" + custName + '\'' +
                '}';
    }
}
