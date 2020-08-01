package com.ppetrov.interview.alm;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

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
        await().atMost(Duration.ofSeconds(1))
                .pollInterval(Duration.ofMillis(1))
                .until(() -> !entityLocker.isLocked(testID));
    }

    @Test
    public void testLockingSameRow() throws InterruptedException {
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
        await().atMost(Duration.ofSeconds(1))
                .pollInterval(Duration.ofMillis(1))
                .until(secondLockAttempt::isDone);
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

        await().atMost(Duration.ofSeconds(1))
                .pollInterval(Duration.ofMillis(1))
                .until(() -> !entityLocker.isLocked(firstID) && !entityLocker.isLocked(secondID));
    }

    @Test
    public void testReentrantLock() throws InterruptedException {
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch reentrantStart = new CountDownLatch(1);
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch reentrantLocked = new CountDownLatch(1);
        CountDownLatch reentrantUnlocked = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);

        EntityLocker<TestID> entityLocker = new EntityLocker<>();
        TestID testID = new TestID();

        assertFalse(entityLocker.isLocked(testID));

        Thread thread = new Thread(() ->
                entityLocker.lockAndRun(testID, () -> {
                    start.countDown();
                    try {
                        locked.await();
                        entityLocker.lockAndRun(testID, () -> {
                            reentrantStart.countDown();
                            try {
                                reentrantLocked.await();
                            } catch (InterruptedException e) {
                                fail("Reentrant locking attempt has been interrupted.");
                            }
                        });
                        reentrantUnlocked.await();
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

        reentrantStart.await();
        assertTrue(entityLocker.isLocked(testID));
        reentrantLocked.countDown();

        assertTrue(entityLocker.isLocked(testID));
        reentrantUnlocked.countDown();

        finish.await();
        await().atMost(Duration.ofSeconds(1))
                .pollInterval(Duration.ofMillis(1))
                .until(() -> !entityLocker.isLocked(testID));
    }

}
