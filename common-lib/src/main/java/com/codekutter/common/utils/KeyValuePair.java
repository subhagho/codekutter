package com.codekutter.common.utils;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public class KeyValuePair<K, V> {
    private K key;
    private V value;
}
