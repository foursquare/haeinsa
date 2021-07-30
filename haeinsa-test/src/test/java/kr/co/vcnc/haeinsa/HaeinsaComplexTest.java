/**
 * Copyright (C) 2013-2015 VCNC Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kr.co.vcnc.haeinsa;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.util.Bytes;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Complex multi-thread unit test for Haeinsa. It contains simple-increment
 * test, concurrent random increment test, and serializability test.
 */
public class HaeinsaComplexTest extends HaeinsaTestBase {

    /**
     * Test which executes multiple transactions which increment specific value by single thread and check result.
     */
    @Test
    public void testSimepleIncrement() throws Exception {
        final HaeinsaTransactionManager tm = context().getTransactionManager();
        final HaeinsaTableIface testTable = context().getHaeinsaTableIface("test");

        HaeinsaTransaction tx;

        final AtomicLong count = new AtomicLong(0);
        final long maxIter = 1000;

        // initial value
        final byte[] row = Bytes.toBytes("count");
        final byte[] CF = Bytes.toBytes("data");
        final byte[] CQ = Bytes.toBytes("value");

        tx = tm.begin();
        HaeinsaPut put = new HaeinsaPut(row);
        put.add(CF, CQ, Bytes.toBytes(0L));
        testTable.put(tx, put);
        tx.commit();

        for (int i = 0; i < maxIter; i++) {
            try {
                tx = tm.begin();
                HaeinsaGet get = new HaeinsaGet(row);
                get.addColumn(CF, CQ);
                long countOnDB = Bytes.toLong(testTable.get(tx, get).getValue(CF, CQ));
                put = new HaeinsaPut(row);
                countOnDB += 1;
                put.add(CF, CQ, Bytes.toBytes(countOnDB));
                testTable.put(tx, put);
                tx.commit();
                count.addAndGet(1L);
            } catch (IOException e) {
                // IOException on HBase operations
            }
        }

        // check result
        tx = tm.begin();
        HaeinsaGet get = new HaeinsaGet(row);
        get.addColumn(CF, CQ);
        long countOnDB = Bytes.toLong(testTable.get(tx, get).getValue(CF, CQ));
        tx.rollback();

        Assert.assertEquals(countOnDB, maxIter);
        Assert.assertEquals(count.get(), maxIter);

        testTable.close();
    }

    /**
     * Test which execute multiple transactions by multiple threads concurrently which increase value of
     * single row randomly. Check result after transactions with value in local variable.
     */
    @Test
    public void testConcurrentRandomIncrement() throws Exception {
        final HaeinsaTransactionManager tm = context().getTransactionManager();
        final HaeinsaTableIface testTable = context().getHaeinsaTableIface("test");

        HaeinsaTransaction tx;

        final AtomicLong count = new AtomicLong(0);
        final long maxIter = 100;
        final int randomRange = 100;
        final int numberOfJob = 10;
        final CountDownLatch countDownLatch = new CountDownLatch(numberOfJob);
        final AtomicLong successCount = new AtomicLong(0);
        final AtomicLong failCount = new AtomicLong(0);

        // initial value
        final byte[] row = Bytes.toBytes("count");
        final byte[] CF = Bytes.toBytes("data");
        final byte[] CQ = Bytes.toBytes("value");

        tx = tm.begin();
        HaeinsaPut put = new HaeinsaPut(row);
        put.add(CF, CQ, Bytes.toBytes(0L));
        testTable.put(tx, put);
        tx.commit();

        Runnable singleIncrementJob = new Runnable() {

            @Override
            public void run() {
                int iteration = 0;
                while (iteration < maxIter) {
                    try {
                        HaeinsaTransaction tx = tm.begin();
                        HaeinsaGet get = new HaeinsaGet(row);
                        get.addColumn(CF, CQ);
                        long countOnDB = Bytes.toLong(testTable.get(tx, get).getValue(CF, CQ));

                        long newIncrement = new Random().nextInt(randomRange);
                        countOnDB += newIncrement;

                        HaeinsaPut put = new HaeinsaPut(row);
                        put.add(CF, CQ, Bytes.toBytes(countOnDB));
                        testTable.put(tx, put);
                        tx.commit();
                        count.addAndGet(newIncrement);
                        iteration++;
                        successCount.getAndIncrement();
                    } catch (IOException e) {
                        failCount.getAndIncrement();
                    }
                }
                System.out.println(String.format("iteration : %d on Thread : %s", iteration, Thread.currentThread().getName()));
                countDownLatch.countDown();
            }
        };

        ExecutorService service = Executors.newFixedThreadPool(numberOfJob,
                new ThreadFactoryBuilder().setNameFormat("IncrementJobThread-%d").build());

        for (int i = 0; i < numberOfJob; i++) {
            service.execute(singleIncrementJob);
        }
        countDownLatch.await();

        // check result
        tx = tm.begin();
        HaeinsaGet get = new HaeinsaGet(row);
        get.addColumn(CF, CQ);
        long countOnDB = Bytes.toLong(testTable.get(tx, get).getValue(CF, CQ));
        tx.rollback();
        System.out.println(countOnDB);

        Assert.assertEquals(countOnDB, count.get());
        System.out.println("Number of Success Transactions : " + successCount.get());
        System.out.println("Number of Failed Transactions : " + failCount.get());
        System.out.println("Conflict rate : " + failCount.get() /
                ((double) failCount.get() + (double) successCount.get()) * 100.0);

        // release resources
        testTable.close();
        service.shutdown();
    }

    /**
     * Test serializability.
     * Start with writing random value in row1 and row2.
     * Execute transaction by multiple threads concurrently.
     * Each transaction will get value from DB and write new value on the row based on previous value.
     * Pseudo code is as follow
     * <pre>
     * db.put (row1, hash( db.get(row1) + random ))
     * db.put (row2, hash( db.get(row2) + random ))
     * </pre>
     * If transaction successes, it will acquire local memory lock and atomically change two atomicInteger in local.
     * After multiple times of concurrent transaction, if data in memory and DB is same then we can think this schedule is serializable.
     */
    //@Test
    public void testSerializability() throws Exception {
        final HaeinsaTransactionManager tm = context().getTransactionManager();
        final HaeinsaTableIface testTable = context().getHaeinsaTableIface("test");

        HaeinsaTransaction tx;

        // some random initial value
        final Object lock = new Object();
        final AtomicLong value1 = new AtomicLong(new Random().nextInt());
        final AtomicLong value2 = new AtomicLong(new Random().nextInt());

        final long maxIter = 10;
        final int numberOfJob = 4;
        final CountDownLatch countDownLatch = new CountDownLatch(numberOfJob);
        final AtomicLong successCount = new AtomicLong(0);
        final AtomicLong failCount = new AtomicLong(0);

        // initial value
        final byte[] CF = Bytes.toBytes("data");
        final byte[] row1 = Bytes.toBytes("row1");
        final byte[] CQ1 = Bytes.toBytes("col1");
        final byte[] row2 = Bytes.toBytes("row2");
        final byte[] CQ2 = Bytes.toBytes("col2");

        System.out.println("Start testSerializability test");
        tx = tm.begin();
        HaeinsaPut put = new HaeinsaPut(row1).add(CF, CQ1, Bytes.toBytes(value1.get()));
        testTable.put(tx, put);
        put = new HaeinsaPut(row2).add(CF, CQ2, Bytes.toBytes(value2.get()));
        testTable.put(tx, put);
        tx.commit();

        /*
         * newValue1 = hashWithRandom( oldValue1 )
         * newValue2 = hashWithRandom( oldValue2 )
         *
         * tx.begin();
         * tx.write(newValue1);
         * tx.write(newValue2);
         * tx.commit();
         */
        Runnable serialJob = new Runnable() {
            @Override
            public void run() {
                int iteration = 0;
                while (iteration < maxIter) {
                    try {
                        HaeinsaTransaction tx = tm.begin();
                        long oldValue1 = Bytes.toLong(testTable.get(tx,
                                new HaeinsaGet(row1).addColumn(CF, CQ1)).getValue(CF, CQ1));
                        long oldValue2 = Bytes.toLong(testTable.get(tx,
                                new HaeinsaGet(row2).addColumn(CF, CQ2)).getValue(CF, CQ2));
                        long newValue1 = nextHashedValue(oldValue1);
                        long newValue2 = nextHashedValue(oldValue2);

                        testTable.put(tx, new HaeinsaPut(row1).add(CF, CQ1, Bytes.toBytes(newValue1)));
                        testTable.put(tx, new HaeinsaPut(row2).add(CF, CQ2, Bytes.toBytes(newValue2)));

                        tx.commit();

                        // success
                        iteration++;
                        successCount.incrementAndGet();
                        synchronized (lock) {
                            Assert.assertTrue(value1.compareAndSet(oldValue1, newValue1));
                            Assert.assertTrue(value2.compareAndSet(oldValue2, newValue2));
                        }
                    } catch (Exception e) {
                        // fail
                        failCount.getAndIncrement();
                    }
                }
                System.out.println(String.format("iteration : %d on Thread : Ts", iteration, Thread.currentThread().getName()));
                countDownLatch.countDown();
            }
        };

        ExecutorService service = Executors.newFixedThreadPool(numberOfJob,
                new ThreadFactoryBuilder().setNameFormat("Serializability-job-thread-%d").build());

        for (int i = 0; i < numberOfJob; i++) {
            service.execute(serialJob);
        }
        countDownLatch.await();

        long dbValue1 = Bytes.toLong(testTable.get(tx, new HaeinsaGet(row1).addColumn(CF, CQ1)).getValue(CF, CQ1));
        long dbValue2 = Bytes.toLong(testTable.get(tx, new HaeinsaGet(row2).addColumn(CF, CQ2)).getValue(CF, CQ2));
        Assert.assertEquals(dbValue1, value1.get());
        Assert.assertEquals(dbValue2, value2.get());
        System.out.println("Number of Success Transactions : " + successCount.get());
        System.out.println("Number of Failed Transactions : " + failCount.get());
        System.out.println("Conflict rate : " + failCount.get() / ((double) failCount.get() + (double) successCount.get()) * 100.0);

        // release resources
        testTable.close();
        service.shutdown();
    }

    /**
     * return (str(oldValue) + str(random int)).hashCode()
     */
    private long nextHashedValue(long oldValue) {
        String result = "";
        result += oldValue;
        result += new Random().nextInt();
        return result.hashCode();
    }
}
