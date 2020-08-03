package com.ppetrov.interview.alm;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Allows to perform both ID-based locking and global locking for running a protected code.
 * Locking is not shared between different instances of EntityLocker class.
 * EntityLocker executes protected code in the calling thread, it does not perform any execution management.
 */
public final class EntityLocker<T extends IEntityID> {

    private final Map<T, ReentrantLock> locksByIDs = new ConcurrentHashMap<>();
    private final ReadWriteLock globalLock = new ReentrantReadWriteLock();

    /**
     * Runs globally locked protected code.
     * If any ID-based locking code is running at the moment, call to globalLock waits for it to finish before starting.
     */
    public final void globalLock(Runnable protectedCode) {
        globalLock.writeLock().lock();
        try {
            protectedCode.run();
        } finally {
            globalLock.writeLock().unlock();
        }
    }

    /**
     * Runs protectedCode, locking it by id.
     * At most one thread can execute protected code on that entity.
     * If there’s a concurrent request to lock the same entity, the other thread waits until the entity becomes available.
     * If the globalLock is acquired, lock-by-id request waits for it to finish its work.
     * Allows reentrant locking.
     */
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

    /**
     * This method is supposed to be used only for testing and debugging purposes.
     * @return true if the specified id is locked by any thread.
     */
    public final boolean isLocked(T id) {
        ReentrantLock lock = locksByIDs.get(id);
        return lock != null && lock.isLocked();
    }

}
