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

package com.codekutter.r2db.driver;

import com.codekutter.common.stores.model.Order;
import com.codekutter.common.utils.LogUtils;
import org.apache.lucene.search.Query;
import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class EntityQueryBuilderTest {

    @Test
    void parse() {
        try {
            EntityQueryBuilder<Order> builder = EntityQueryBuilder.builder(Order.class);
            SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy");
            String query = builder.group()
                    .equals("customer.lastName", "surname").notEquals("customer.firstName", "unknown").end()
                    .range("createdOn", df.format(new Date(System.currentTimeMillis() - 10000000)), df.format(new Date()))
                    .notGroup()
                    .gte("items.quantity", "5").lte("items.quantity", "30")
                    .in("items.id.productId", new String[]{"PRODUCT_1", "PRODUCT_2", "PRODUCT_3"})
                    .end()
                    .parse();
            LogUtils.debug(getClass(), String.format("query=[%s]", query));
        } catch (Throwable t) {
            LogUtils.error(getClass(), t);
            fail(t);
        }
    }
}