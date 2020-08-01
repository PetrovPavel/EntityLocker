package com.ppetrov.interview.alm;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public final class EntityLocker<T extends IEntityID> {

    private final Map<T, ReentrantLock> locksByIDs = new ConcurrentHashMap<>();

    public final void lockAndRun(T id, Runnable protectedCode) {
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        try {
            ReentrantLock existingLock = locksByIDs.putIfAbsent(id, lock);
            try {
                if (existingLock != null) {
                    existingLock.lock();
                }
                protectedCode.run();
            } finally {
                if (existingLock != null) {
                    existingLock.unlock();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public final boolean isLocked(T id) {
        ReentrantLock lock = locksByIDs.get(id);
        return lock != null && lock.isLocked();
    }

}
