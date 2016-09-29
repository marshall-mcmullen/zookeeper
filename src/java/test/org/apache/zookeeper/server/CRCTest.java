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

package org.apache.zookeeper.server;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.InputArchive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.server.persistence.FileSnap;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.server.persistence.FileTxnLog.FileTxnIterator;
import org.apache.zookeeper.server.persistence.Util;
import org.apache.zookeeper.server.persistence.TxnLog.TxnIterator;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Assert;
import org.junit.Test;

public class CRCTest extends ZKTestCase implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(CRCTest.class);

    private static final String HOSTPORT =
        "127.0.0.1:" + PortAssignment.unique();
    private volatile CountDownLatch startSignal;

    /**
     * corrupt a file by writing m at 500 b
     * offset
     * @param file the file to be corrupted
     * @throws IOException
     */
    private void corruptFile(File file) throws IOException {
        // corrupt the logfile
        RandomAccessFile raf  = new RandomAccessFile(file, "rw");
        byte[] b = "mahadev".getBytes();
        long writeLen = 500L;
        raf.seek(writeLen);
        //corrupting the data
        raf.write(b);
        raf.close();
    }
    
    /**
     * Corrupt a log file by writing data into it relative to the beginning of the third
     * record. This allows precise control over placing the corruption in the tail of the 
     * second record or the head of the third. The first record should be successfully read
     * to ensure any error is really the result of intentional corruption.
     * @param file The path to the log file 
     * @param offset Byte offset from the beginning of 3rd record. -1 is 2nd record sentinel
     * @param data Bytes of data to write to the log
     * @throws IOException
     */
    private void corruptLog(File file, int offset, byte[] data) throws IOException {
        // To find the 3rd record we start reading the log stream at the first zxid in this
        // log. Then we just read two records and we are at the 3rd record
        FileTxnLog flog = new FileTxnLog(file.getParentFile());
        long zxid = Util.getZxidFromName(file.getName(), "log");
        FileTxnIterator itr = (FileTxnIterator) flog.read(zxid);
        itr.next(); //skip first record
        itr.next(); //skip 2nd record
        long position = itr.getPosition()+offset; // the byte to start corruption
        itr.close(); // close the log stream

        // corrupt the log file
        RandomAccessFile raf  = new RandomAccessFile(file, "rw");
        raf.seek(position);
        raf.write(data);
        raf.close();
    }

    /** return if checksum matches for a snapshot **/
    private boolean getCheckSum(FileSnap snap, File snapFile) throws IOException {
        DataTree dt = new DataTree();
        Map<Long, Integer> sessions = new ConcurrentHashMap<Long, Integer>();
        InputStream snapIS = new BufferedInputStream(new FileInputStream(
                snapFile));
        CheckedInputStream crcIn = new CheckedInputStream(snapIS, new Adler32());
        InputArchive ia = BinaryInputArchive.getArchive(crcIn);
        try {
            snap.deserialize(dt, sessions, ia);
        } catch (IOException ie) {
            // we failed on the most recent snapshot
            // must be incomplete
            // try reading the next one
            // after corrupting
            snapIS.close();
            crcIn.close();
            throw ie;
        }

        long checksum = crcIn.getChecksum().getValue();
        long val = ia.readLong("val");
        snapIS.close();
        crcIn.close();
        return (val != checksum);
    }

    /** test checksums for the logs and snapshots.
     * the reader should fail on reading
     * a corrupt snapshot and a corrupt log
     * file
     * @throws Exception
     */
    @Test
    public void testChecksums() throws Exception {
        File tmpDir = ClientBase.createTmpDir();
        ClientBase.setupTestEnv();
        ZooKeeperServer zks = new ZooKeeperServer(tmpDir, tmpDir, 3000);
        SyncRequestProcessor.setSnapCount(150);
        final int PORT = Integer.parseInt(HOSTPORT.split(":")[1]);
        ServerCnxnFactory f = ServerCnxnFactory.createFactory(PORT, -1);
        f.startup(zks);
        LOG.info("starting up the zookeeper server .. waiting");
        Assert.assertTrue("waiting for server being up",
                ClientBase.waitForServerUp(HOSTPORT,CONNECTION_TIMEOUT));
        ZooKeeper zk = new ZooKeeper(HOSTPORT, CONNECTION_TIMEOUT, this);
        try {
            for (int i =0; i < 2000; i++) {
                zk.create("/crctest- " + i , ("/crctest- " + i).getBytes(),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } finally {
            zk.close();
        }
        f.shutdown();
        Assert.assertTrue("waiting for server down",
                   ClientBase.waitForServerDown(HOSTPORT,
                           ClientBase.CONNECTION_TIMEOUT));

        // Find the names of all the log files generated in reverse xid order
        File versionDir = new File(tmpDir, "version-2");
        List<File> files = Util.sortDataDir(versionDir.listFiles(), "log", false);
        
        // Save the next to last four logs for four different corruption 
        // experiments. The logs must be corrupted in decreasing zxid order
        // because FileTxnLog starts at the log before the target zxid.
        // Throw away log with highest zxid since it may not be full enough.
        files.get(0).delete();
        File eofLog = files.get(1); // this is the last log that exists now
        LOG.info("sentinel eof log file is " + eofLog);
        File crcLog = files.get(2);
        LOG.info("crc log file is " + crcLog);
        File lenLog = files.get(3);
        LOG.info("bad length log file is " + lenLog);
        File sntLog = files.get(4);
        LOG.info("sentinel log file is " + sntLog);

        // zero the sentinel and other data of the 2nd log record to force an eof. This
        // should not get a crc error despite bad data since the sentinel is zero. Note
        // that we deleted the only log that follows so this is the end of all logs
        byte[] eofSentinel = new byte[16]; // bytes of zero
        corruptLog(eofLog, -eofSentinel.length, eofSentinel); // zero the end of 2nd record
        FileTxnLog flog = new FileTxnLog(versionDir);
        TxnIterator itr = flog.read(Util.getZxidFromName(eofLog.getName(), "log"));
        Assert.assertTrue(itr.next()); //skip first good record
        Assert.assertFalse(itr.next()); //there should be no second record
        itr.close();

        // corrupt the middle of 3rd log record to force a CRC error
        corruptLog(crcLog, 12, "mahadev".getBytes()); // 12 bytes in to skip crc and txlen
        flog = new FileTxnLog(versionDir);
        itr = flog.read(Util.getZxidFromName(crcLog.getName(), "log"));
        Assert.assertTrue(itr.next()); //skip first good record
        Assert.assertTrue(itr.next()); //skip 2nd good record
        try {
            itr.next(); // this should throw an I/O exception because we corrupted data
            Assert.assertTrue(false);
        } catch(IOException ie) {
            // ensure this is the error we expected
            Assert.assertTrue(ie.getMessage().equals("CRC check failed"));
            LOG.info("crc corruption", ie);
        }
        itr.close();

        // corrupt the length of 3rd log record to force a CRC error. Set length
        // to one billion on the presumption that is too big a record.
        byte[] badlen = {
                (byte) ((1000000000 >>> 24) & 0xFF),
                (byte) ((1000000000 >>> 16) & 0xFF),
                (byte) ((1000000000 >>>  8) & 0xFF),
                (byte) ((1000000000 >>>  0) & 0xFF),
        };
        corruptLog(lenLog, 8, badlen); // 8 bytes in to skip crc
        flog = new FileTxnLog(versionDir);
        itr = flog.read(Util.getZxidFromName(lenLog.getName(), "log"));
        Assert.assertTrue(itr.next()); //skip first good record
        Assert.assertTrue(itr.next()); //skip 2nd good record
        try {
            itr.next(); // this should throw an I/O exception because we corrupted data
            Assert.assertTrue(false);
        } catch(IOException ie) {
            // ensure this is the error we expected
            Assert.assertTrue(ie.getMessage().equals("Unreasonable length = 1000000000"));
            LOG.info("length corruption", ie);
        }
        itr.close();

        // corrupt the sentinel of the 2nd log record to force an error
        byte[] badSentinel = { 0x54 }; // the question was "what is 6 times 9"
        corruptLog(sntLog, -1, badSentinel); // corrupt the byte before 3rd record
        flog = new FileTxnLog(versionDir);
        itr = flog.read(Util.getZxidFromName(sntLog.getName(), "log"));
        Assert.assertTrue(itr.next()); //skip first good record
        try {
            itr.next(); // this should throw an I/O exception because we corrupted sentinel
            Assert.assertTrue(false);
        } catch(IOException ie) {
            // ensure this is the error we expected
            Assert.assertTrue(ie.getMessage().equals("Corrupt Sentinel byte"));
            LOG.info("sentinel corruption", ie);
        }
        itr.close();

        // find the last snapshot
        FileSnap snap = new FileSnap(versionDir);
        List<File> snapFiles = snap.findNRecentSnapshots(2);
        File snapFile = snapFiles.get(0);
        corruptFile(snapFile);
        boolean cfile = false;
        try {
            cfile = getCheckSum(snap, snapFile);
        } catch(IOException ie) {
            //the last snapshot seems incompelte
            // corrupt the last but one
            // and use that
            snapFile = snapFiles.get(1);
            corruptFile(snapFile);
            cfile = getCheckSum(snap, snapFile);
        }
        Assert.assertTrue(cfile);
   }

    public void process(WatchedEvent event) {
        LOG.info("Event:" + event.getState() + " " + event.getType() + " " + event.getPath());
        if (event.getState() == KeeperState.SyncConnected
                && startSignal != null && startSignal.getCount() > 0)
        {
            startSignal.countDown();
        }
    }
}
