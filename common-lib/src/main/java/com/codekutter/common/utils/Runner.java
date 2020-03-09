package com.codekutter.common.utils;

public abstract class Runner implements Runnable {
    @Override
    public void run() {
        try {
            doRun();
        } catch (Throwable th) {
            Thread t = Thread.currentThread();
            if (t instanceof ManagedThread) {
                ((ManagedThread) t).errorEvent(getClass(), th);
            }
        }
    }

    public abstract void doRun() throws Exception;
}
