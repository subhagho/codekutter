package com.codekutter.common.utils;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;

@Getter
@Setter
@Accessors(fluent = true)
public class KeyValuePair<K, V> {
    private K key;
    private V value;

    public KeyValuePair() {}

    public KeyValuePair(@Nonnull K key, V value) {
        this.key = key;
        this.value = value;
    }
}
