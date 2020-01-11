package com.codekutter.common.stores.model;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

@Getter
@Setter
@Embeddable
public class ItemId implements Serializable {
    @Column(name = "order_id")
    private String orderId;
    @Column(name = "product_id")
    private String productId;

    public int compareTo(ItemId key) {
        int ret = orderId.compareTo(key.orderId);
        if (ret == 0) {
            ret = productId.compareTo(key.productId);
        }
        return ret;
    }
}
