/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.common;

import junit.framework.Assert;
import org.apache.zookeeper.*;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class TimeTest extends ClientBase {
    private static final long mt0 = System.currentTimeMillis();
    private static final long nt0 = Time.currentElapsedTime();
    
    private static AtomicInteger watchCount = new AtomicInteger(0);

    @Test 
    public void testCurrentElapsedTime() throws InterruptedException {
        long mt0 = System.currentTimeMillis();
        long nt0 = Time.currentElapsedTime();

        Thread.sleep(200);

        Assert.assertEquals(Time.currentElapsedTime() - nt0, System.currentTimeMillis() - mt0, 10);
    }

    /**
     * Command line program for demonstrating robustness to clock changes.
     *
     * While running this, try changing the clock forward using something like [date -s "+1hour"].
     * On a zookeeper that uses currentTimeMillis(), this will cause all ephemerals to be deleted
     * due to session expiration.  With the use of nanoTime instead, zookeeper is unphased.
     *
     * @param args  Not used.
     * @throws Exception Not really.  In fact, the only exceptions are InterruptedException, KeeperException
     * and IOException, but setUp claims to throw more than it ought to.
     */
    public static void main(String[] args) throws Exception {
        System.out.printf("Starting\n");
        final TimeTest test = new TimeTest();
        System.out.printf("After construct\n");
        test.setUp();
        ZooKeeper zk = test.createClient();
        zk.create("/ephemeral", new byte[]{1, 2, 3}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        while (Time.currentElapsedTime() - nt0 < 100000) {
            System.out.printf("%d\t%s\n", discrepancy(), zk.exists("/ephemeral", watchCount.get() == 0 ? createWatcher() : null) != null);
            waitByYielding(500);
        }
    }

    private static Watcher createWatcher() {
        watchCount.incrementAndGet();
        return new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                watchCount.decrementAndGet();
                System.out.printf("%d event = %s\n", discrepancy(), event);
            }
        };

    }

    private static void waitByYielding(long delay) {
        long t0 = Time.currentElapsedTime();
        while (Time.currentElapsedTime() < t0 + delay) {
            Thread.yield();
        }
    }

    private static long discrepancy() {
        return (System.currentTimeMillis() - mt0) - (Time.currentElapsedTime() - nt0);
    }
}
