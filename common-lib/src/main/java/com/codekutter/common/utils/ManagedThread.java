package com.codekutter.common.utils;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class ManagedThread extends Thread {
    private Set<IThreadListener> fIThreadListeners = new HashSet<>();

    public ManagedThread register(@Nonnull IThreadListener pIThreadListener) {
        fIThreadListeners.add(pIThreadListener);
        return this;
    }

    @Override
    public synchronized void start() {
        if (!fIThreadListeners.isEmpty()) {
            for(IThreadListener listener : fIThreadListeners) {
                listener.event(EThreadEvent.Start, this);
            }
        }
        super.start();
    }

    @Override
    public void run() {
        if (!fIThreadListeners.isEmpty()) {
            for(IThreadListener listener : fIThreadListeners) {
                listener.event(EThreadEvent.Run, this);
            }
        }
        super.run();
        if (!fIThreadListeners.isEmpty()) {
            for(IThreadListener listener : fIThreadListeners) {
                listener.event(EThreadEvent.Stop, this);
            }
        }
    }

    @Override
    public void interrupt() {
        if (!fIThreadListeners.isEmpty()) {
            for(IThreadListener listener : fIThreadListeners) {
                listener.event(EThreadEvent.Interrupted, this);
            }
        }
        super.interrupt();
    }

    public void errorEvent(Class<?> type, Throwable error) {
        if (!fIThreadListeners.isEmpty()) {
            for(IThreadListener listener : fIThreadListeners) {
                listener.event(EThreadEvent.Error, this, type, error);
            }
        }
    }
}
