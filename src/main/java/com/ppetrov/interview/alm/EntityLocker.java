package com.ppetrov.interview.alm;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class EntityLocker<T extends IEntityID> {

    private final Map<T, ReentrantLock> locksByIDs = new ConcurrentHashMap<>();
    private final ReadWriteLock globalLock = new ReentrantReadWriteLock();

    public final void globalLock(Runnable protectedCode) {
        globalLock.writeLock().lock();
        try {
            protectedCode.run();
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    public final void lockAndRun(T id, Runnable protectedCode) {
        globalLock.readLock().lock();
        try {
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
        } finally {
            globalLock.readLock().unlock();
        }
    }

    public final boolean isLocked(T id) {
        ReentrantLock lock = locksByIDs.get(id);
        return lock != null && lock.isLocked();
    }

}
