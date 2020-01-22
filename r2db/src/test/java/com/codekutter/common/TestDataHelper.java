package com.codekutter.common;

import com.codekutter.common.stores.model.*;

import java.util.*;

public class TestDataHelper {
    public static List<Order> createData(int count, int itemCount) {
        List<Order> orders = new ArrayList<>();
        List<Product> products = new ArrayList<>(itemCount);
        for (int ii = 0; ii < itemCount; ii++) {
            Product product = createProduct(ii);
            products.add(product);
        }
        for (int ii = 0; ii < count; ii++) {
            Order order = new Order();
            order.setId(UUID.randomUUID().toString());
            order.setCustomer(createCustomer());
            for (int jj = 0; jj < itemCount; jj++) {
                Product product = products.get(jj);
                ItemId id = new ItemId();
                id.setOrderId(order.getId());
                id.setProductId(product.getId());
                Item item = new Item();
                item.setId(id);
                item.setQuantity(jj);
                item.setUnitPrice(product.getBasePrice());

                order.addItem(item);
            }
            order.setCreatedOn(new Date());
            orders.add(order);
        }
        return orders;
    }

    public static List<Order> createData(List<Product> products, int orderCount) {
        List<Order> orders = new ArrayList<>(orderCount);
        for (int ii = 0; ii < orderCount; ii++) {
            Order order = new Order();
            order.setId(UUID.randomUUID().toString());
            order.setCustomer(createCustomer());
            for (int jj = 0; jj < products.size(); jj++) {
                Product product = products.get(jj);
                ItemId id = new ItemId();
                id.setOrderId(order.getId());
                id.setProductId(product.getId());
                Item item = new Item();
                item.setId(id);
                item.setQuantity(jj);
                item.setUnitPrice(product.getBasePrice());

                order.addItem(item);
            }
            order.setCreatedOn(new Date());
            orders.add(order);
        }
        return orders;
    }

    public static List<Product> createProducts(int count) {
        List<Product> products = new ArrayList<>(count);
        for (int ii = 0; ii < count; ii++) {
            Product product = createProduct(ii);
            products.add(product);
        }
        return products;
    }

    public static Customer createCustomer() {
        Customer customer = new Customer();
        customer.setId(UUID.randomUUID().toString());
        customer.setFirstName("First");
        customer.setLastName("Name");
        customer.setDateOfBirth(new Date());
        customer.setEmailId("first.name@email.com");
        customer.setPhoneNumber("+91 12345-67899");
        return customer;
    }

    public static Product createProduct(int index) {
        Random rnd = new Random(System.currentTimeMillis());
        Product product = new Product();
        product.setId(UUID.randomUUID().toString());
        product.setName(String.format("PRODUCT_%d", index));
        product.setDescription(String.format("This is a test product. [name=%s]", product.getName()));
        product.setBasePrice(rnd.nextDouble() * index);
        product.setCreatedDate(System.currentTimeMillis());
        return product;
    }
}
