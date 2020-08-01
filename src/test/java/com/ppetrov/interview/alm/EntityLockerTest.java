package com.ppetrov.interview.alm;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import static org.junit.Assert.*;

public class EntityLockerTest {

    @Test
    public void testIsLocked() throws InterruptedException {
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);

        EntityLocker<TestID> entityLocker = new EntityLocker<>();
        TestID testID = new TestID();

        assertFalse(entityLocker.isLocked(testID));

        Thread thread = new Thread(() ->
                entityLocker.lockAndRun(testID, () -> {
                    start.countDown();
                    try {
                        locked.await();
                    } catch (InterruptedException e) {
                        fail("Locking attempt has been interrupted.");
                    } finally {
                        finish.countDown();
                    }
                }));
        thread.start();

        start.await();
        assertTrue(entityLocker.isLocked(testID));
        locked.countDown();

        finish.await();
        assertFalse(entityLocker.isLocked(testID));
    }

    @Test
    public void testLockingSameRow() throws InterruptedException, ExecutionException {
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch locked = new CountDownLatch(1);

        EntityLocker<TestID> entityLocker = new EntityLocker<>();
        TestID testID = new TestID();

        Thread threadOne = new Thread(() ->
                entityLocker.lockAndRun(testID, () -> {
                    start.countDown();
                    try {
                        locked.await();
                    } catch (InterruptedException e) {
                        fail("Locking attempt has been interrupted.");
                    }
                }));
        threadOne.start();

        start.await();
        assertTrue(entityLocker.isLocked(testID));

        FutureTask<Boolean> secondLockAttempt = new FutureTask<>(() -> {
            entityLocker.lockAndRun(testID, () -> {
            });
            return true;
        });
        new Thread(secondLockAttempt).start();

        // While first thread is locking the ID, second thread should wait
        assertFalse(secondLockAttempt.isDone());

        locked.countDown();
        assertTrue(secondLockAttempt.get());
    }

    @Test
    public void testLockingDifferentRows() throws InterruptedException {
        CountDownLatch start = new CountDownLatch(2);
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(2);

        EntityLocker<TestID> entityLocker = new EntityLocker<>();
        TestID firstID = new TestID();
        TestID secondID = new TestID();

        Thread threadOne = new Thread(() ->
                entityLocker.lockAndRun(firstID, () -> {
                    start.countDown();
                    try {
                        locked.await();
                    } catch (InterruptedException e) {
                        fail("First locking attempt has been interrupted.");
                    } finally {
                        finish.countDown();
                    }
                }));
        Thread threadTwo = new Thread(() ->
                entityLocker.lockAndRun(secondID, () -> {
                    start.countDown();
                    try {
                        locked.await();
                    } catch (InterruptedException e) {
                        fail("Second locking attempt has been interrupted.");
                    } finally {
                        finish.countDown();
                    }
                }));
        threadOne.start();
        threadTwo.start();

        start.await();
        assertTrue(entityLocker.isLocked(firstID));
        assertTrue(entityLocker.isLocked(secondID));

        locked.countDown();
        finish.await();

        assertFalse(entityLocker.isLocked(firstID));
        assertFalse(entityLocker.isLocked(secondID));
    }

}
