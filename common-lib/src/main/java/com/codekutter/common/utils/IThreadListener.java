package com.codekutter.common.utils;

public interface IThreadListener {
    void event(EThreadEvent event, Thread thread, Object...params);
}
