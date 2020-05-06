package com.codekutter.common.utils;

import com.codekutter.zconfig.common.BaseConfigEnv;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;

@Getter
@Setter
@Accessors(fluent = true)
public class ManagedThread extends Thread {
    @Setter(AccessLevel.NONE)
    private final ProcessState state = new ProcessState();
    @Setter(AccessLevel.NONE)
    private Set<IThreadListener> fIThreadListeners = new HashSet<>();
    @Setter(AccessLevel.NONE)
    private Runnable runnable;

    public ManagedThread(Runnable target, String name) {
        super(target, name);
        runnable = target;
    }

    public ManagedThread(ThreadGroup group, Runnable target, String name) {
        super(group, target, name);
        runnable = target;
    }

    public ManagedThread(ThreadGroup group, Runnable target, String name, long stackSize) {
        super(group, target, name, stackSize);
        runnable = target;
    }

    public ManagedThread register(@Nonnull IThreadListener pIThreadListener) {
        fIThreadListeners.add(pIThreadListener);
        return this;
    }

    @Override
    public synchronized void start() {
        if (!fIThreadListeners.isEmpty()) {
            for (IThreadListener listener : fIThreadListeners) {
                listener.event(EThreadEvent.Start, this);
            }
        }
        super.start();
    }

    @Override
    public void run() {
        if (!fIThreadListeners.isEmpty()) {
            for (IThreadListener listener : fIThreadListeners) {
                listener.event(EThreadEvent.Run, this);
            }
        }
        try {
            super.run();
        } catch (Throwable t) {
            Class<?> type = getClass();
            if (runnable != null) {
                type = runnable.getClass();
            }
            errorEvent(type, t);
        } finally {
            if (!fIThreadListeners.isEmpty()) {
                for (IThreadListener listener : fIThreadListeners) {
                    listener.event(EThreadEvent.Stop, this);
                }
            }
            try {
                if (!BaseConfigEnv.env().remove(this)) {
                    LogUtils.warn(getClass(), String.format("Thread not registered. [ID=%d]", getId()));
                }
            } catch (Exception ex) {
                LogUtils.error(getClass(), ex);
            }
        }
    }

    @Override
    public void interrupt() {
        if (!fIThreadListeners.isEmpty()) {
            for (IThreadListener listener : fIThreadListeners) {
                listener.event(EThreadEvent.Interrupted, this);
            }
        }
        super.interrupt();
    }

    public void errorEvent(Class<?> type, Throwable error) {
        if (!fIThreadListeners.isEmpty()) {
            for (IThreadListener listener : fIThreadListeners) {
                listener.event(EThreadEvent.Error, this, type, error);
            }
        }
    }
}
