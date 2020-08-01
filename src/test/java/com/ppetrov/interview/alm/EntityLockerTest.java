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

    @Test
    public void testGlobalLock() throws InterruptedException {
        EntityLocker<TestID> entityLocker = new EntityLocker<>();
        TestID testID = new TestID();

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch codeRunning = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        new Thread(() ->
                entityLocker.lockAndRun(testID, () -> {
                    start.countDown();
                    try {
                        codeRunning.await();
                    } catch (InterruptedException e1) {
                        fail("Locking attempt before global lock has been interrupted.");
                    } finally {
                        finish.countDown();
                    }
                })).start();
        start.await();

        CountDownLatch globalLockStart = new CountDownLatch(1);
        CountDownLatch globalCodeRunning = new CountDownLatch(1);
        CountDownLatch globalLockFinish = new CountDownLatch(1);
        new Thread(() ->
                entityLocker.globalLock(() -> {
                    globalLockStart.countDown();
                    try {
                        globalCodeRunning.await();
                    } catch (InterruptedException e) {
                        fail("Global locking attempt has been interrupted.");
                    } finally {
                        globalLockFinish.countDown();
                    }
                })).start();

        codeRunning.countDown();
        finish.await();
        globalLockStart.await();

        CountDownLatch afterGlobalCodeRunning = new CountDownLatch(1);
        CountDownLatch afterGlobalLockFinish = new CountDownLatch(1);
        new Thread(() ->
                entityLocker.lockAndRun(testID, () -> {
                    try {
                        afterGlobalCodeRunning.await();
                    } catch (InterruptedException e1) {
                        fail("Locking attempt after global lock has been interrupted.");
                    } finally {
                        afterGlobalLockFinish.countDown();
                    }
                })).start();

        assertFalse(entityLocker.isLocked(testID));

        globalCodeRunning.countDown();
        globalLockFinish.await();
        await().atMost(Duration.ofSeconds(1))
                .pollInterval(Duration.ofMillis(1))
                .until(() -> entityLocker.isLocked(testID));

        afterGlobalCodeRunning.countDown();
        afterGlobalLockFinish.await();
        await().atMost(Duration.ofSeconds(1))
                .pollInterval(Duration.ofMillis(1))
                .until(() -> !entityLocker.isLocked(testID));
    }

}
