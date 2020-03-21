/*
 *  Copyright (2020) Subhabrata Ghosh (subho dot ghosh at outlook dot com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.codekutter.common;

import com.codekutter.common.stores.model.*;
import com.google.common.base.Strings;

import java.util.*;

public class TestDataHelper {
    public static List<Order> createData(int count, int itemCount, String prefix) {
        List<Order> orders = new ArrayList<>();
        List<Product> products = new ArrayList<>(itemCount);
        for (int ii = 0; ii < itemCount; ii++) {
            Product product = createProduct(ii, prefix);
            products.add(product);
        }
        for (int ii = 0; ii < count; ii++) {
            Order order = new Order();
            order.setId(new OrderKey(UUID.randomUUID().toString()));
            order.setCustomer(createCustomer());
            for (int jj = 0; jj < itemCount; jj++) {
                Product product = products.get(jj);
                ItemId id = new ItemId();
                id.setOrderId(order.getId().getKey());
                id.setProductId(product.getId().getKey());
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
            order.setId(new OrderKey(UUID.randomUUID().toString()));
            order.setCustomer(createCustomer());
            for (int jj = 0; jj < products.size(); jj++) {
                Product product = products.get(jj);
                ItemId id = new ItemId();
                id.setOrderId(order.getId().getKey());
                id.setProductId(product.getId().getKey());
                Item item = new Item();
                item.setId(id);
                item.setQuantity(jj);
                item.setUnitPrice(product.getBasePrice());

                order.addItem(item);
            }
            order.setCreatedOn(new Date());
            List<String> strings = new ArrayList<>();
            for (int jj = 0; jj < ii + 5; jj++) {
                strings.add(UUID.randomUUID().toString());
            }
            order.setTestEncryptionList(strings);
            Set<Customer> customers = new HashSet<>();
            customers.add(order.getCustomer());
            order.setTestSetEmbedded(customers);
            orders.add(order);
        }
        return orders;
    }

    public static List<Product> createProducts(int count, String prefix) {
        List<Product> products = new ArrayList<>(count);
        for (int ii = 0; ii < count; ii++) {
            Product product = createProduct(ii, prefix);
            products.add(product);
        }
        return products;
    }

    public static Customer createCustomer() {
        Customer customer = new Customer();
        customer.setId(new CustomerKey(UUID.randomUUID().toString()));
        customer.setFirstName("First");
        customer.setLastName("Name");
        customer.setDateOfBirth(new Date());
        customer.setEmailId("first.name@email.com");
        customer.setPhoneNumber("+91 12345-67899");
        return customer;
    }

    public static Product createProduct(int index, String prefix) {
        if (Strings.isNullOrEmpty(prefix)) {
            prefix = "PRODUCT_";
        }
        Random rnd = new Random(System.currentTimeMillis());
        Product product = new Product();
        product.setId(new ProductKey(UUID.randomUUID().toString()));
        product.setName(String.format("%s%d", prefix, index));
        product.setDescription(String.format("This is a test product. [name=%s]", product.getName()));
        product.setBasePrice(rnd.nextDouble() * index);
        product.setCreatedDate(System.currentTimeMillis());
        return product;
    }
}
