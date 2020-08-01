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
        EntityLocker<TestID> entityLocker = new EntityLocker<>();
        TestID testID = new TestID();

        assertFalse(entityLocker.isLocked(testID));

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch codeRunning = new CountDownLatch(1);
        new Thread(() ->
                entityLocker.lockAndRun(testID, () -> {
                    start.countDown();
                    try {
                        codeRunning.await();
                    } catch (InterruptedException e) {
                        fail("Locking attempt has been interrupted.");
                    }
                })).start();
        start.await();

        assertTrue(entityLocker.isLocked(testID));

        codeRunning.countDown();
        await().atMost(Duration.ofSeconds(1))
                .pollInterval(Duration.ofMillis(1))
                .until(() -> !entityLocker.isLocked(testID));
    }

    @Test
    public void testLockingSameRow() throws InterruptedException {
        EntityLocker<TestID> entityLocker = new EntityLocker<>();
        TestID testID = new TestID();

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch codeRunning = new CountDownLatch(1);
        new Thread(() ->
                entityLocker.lockAndRun(testID, () -> {
                    start.countDown();
                    try {
                        codeRunning.await();
                    } catch (InterruptedException e) {
                        fail("Locking attempt has been interrupted.");
                    }
                })).start();
        start.await();

        FutureTask<Boolean> secondLockAttempt = new FutureTask<>(() -> {
            entityLocker.lockAndRun(testID, () -> {
            });
            return true;
        });
        new Thread(secondLockAttempt).start();

        assertFalse(secondLockAttempt.isDone());

        codeRunning.countDown();
        await().atMost(Duration.ofSeconds(1))
                .pollInterval(Duration.ofMillis(1))
                .until(secondLockAttempt::isDone);
    }

    @Test
    public void testLockingDifferentRows() throws InterruptedException {
        EntityLocker<TestID> entityLocker = new EntityLocker<>();
        TestID firstID = new TestID();
        TestID secondID = new TestID();

        CountDownLatch start = new CountDownLatch(2);
        CountDownLatch codeRunning = new CountDownLatch(1);
        new Thread(() ->
                entityLocker.lockAndRun(firstID, () -> {
                    start.countDown();
                    try {
                        codeRunning.await();
                    } catch (InterruptedException e) {
                        fail("First locking attempt has been interrupted.");
                    }
                })).start();
        new Thread(() ->
                entityLocker.lockAndRun(secondID, () -> {
                    start.countDown();
                    try {
                        codeRunning.await();
                    } catch (InterruptedException e) {
                        fail("Second locking attempt has been interrupted.");
                    }
                })).start();
        start.await();

        assertTrue(entityLocker.isLocked(firstID));
        assertTrue(entityLocker.isLocked(secondID));

        codeRunning.countDown();

        await().atMost(Duration.ofSeconds(1))
                .pollInterval(Duration.ofMillis(1))
                .until(() -> !entityLocker.isLocked(firstID)
                        && !entityLocker.isLocked(secondID));
    }

    @Test
    public void testReentrantLock() throws InterruptedException {
        EntityLocker<TestID> entityLocker = new EntityLocker<>();
        TestID testID = new TestID();

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch reentrantStart = new CountDownLatch(1);
        CountDownLatch codeRunning = new CountDownLatch(1);
        CountDownLatch reentrantCodeRunning = new CountDownLatch(1);
        CountDownLatch reentrantFinish = new CountDownLatch(1);
        new Thread(() ->
                entityLocker.lockAndRun(testID, () -> {
                    start.countDown();
                    try {
                        codeRunning.await();
                        entityLocker.lockAndRun(testID, () -> {
                            reentrantStart.countDown();
                            try {
                                reentrantCodeRunning.await();
                            } catch (InterruptedException e) {
                                fail("Reentrant locking attempt has been interrupted.");
                            }
                        });
                        reentrantFinish.await();
                    } catch (InterruptedException e) {
                        fail("Locking attempt has been interrupted.");
                    }
                })).start();
        start.await();

        assertTrue(entityLocker.isLocked(testID));

        codeRunning.countDown();
        reentrantStart.await();
        assertTrue(entityLocker.isLocked(testID));

        reentrantCodeRunning.countDown();
        assertTrue(entityLocker.isLocked(testID));

        reentrantFinish.countDown();
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

        CountDownLatch codeRunningAfterGlobal = new CountDownLatch(1);
        CountDownLatch codeFinishedAfterGlobal = new CountDownLatch(1);
        new Thread(() ->
                entityLocker.lockAndRun(testID, () -> {
                    try {
                        codeRunningAfterGlobal.await();
                    } catch (InterruptedException e1) {
                        fail("Locking attempt after global lock has been interrupted.");
                    } finally {
                        codeFinishedAfterGlobal.countDown();
                    }
                })).start();

        assertFalse(entityLocker.isLocked(testID));

        globalCodeRunning.countDown();
        globalLockFinish.await();
        await().atMost(Duration.ofSeconds(1))
                .pollInterval(Duration.ofMillis(1))
                .until(() -> entityLocker.isLocked(testID));

        codeRunningAfterGlobal.countDown();
        codeFinishedAfterGlobal.await();
        await().atMost(Duration.ofSeconds(1))
                .pollInterval(Duration.ofMillis(1))
                .until(() -> !entityLocker.isLocked(testID));
    }

}
