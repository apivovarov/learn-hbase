
package org.x4444.hbase.S01;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

public class L2 {

    static final byte[] MIN = Bytes.toBytes("min");

    static final byte[] MAX = Bytes.toBytes("max");

    static final byte[] F1 = Bytes.toBytes("f1");

    static final int minMetric = 50001;

    static final int metricN = 100;

    static final long startTsSec = 1388534400;

    // offset - # threads * min to write of prev run
    static final int tsOffsetSec = 60 * 32 * 10000;

    static final boolean debug = false;

    final Random rnd = new Random();

    public static void main(String[] args) {
        final L2 l2 = new L2();

        String op = "rs";

        if (op.equals("rs")) {
            String rowKey = "500021407734340";
            l2.readSingleValue(rowKey);
        } else if (op.equals("rm")) {
            int nT = 10;
            final int readN = 10000;
            Thread[] tt = new Thread[nT];
            for (int i = 0; i < nT; i++) {
                tt[i] = new Thread() {
                    public void run() {
                        L2 ll2 = new L2();
                        ll2.readRandomValues(readN);
                    }
                };
            }

            long ts = System.currentTimeMillis();
            for (Thread t : tt) {
                t.start();
            }
            for (Thread t : tt) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            ts = System.currentTimeMillis() - ts;
            System.out.println("--------------------------------------------");
            System.out.println("N of Threads: " + nT);
            System.out.println("Reads in each Threads: " + readN);
            System.out.println("All Threads time ms: " + ts);
            System.out.printf("All Threads Get per sec: %.2f\n", nT * readN / (ts / 1000D));
            System.out.printf("total rows: %,d\n", nT * readN);
        } else if (op.equals("w")) {

            int nT = 10;

            final int minToWrite = 1000;

            Thread[] tt = new Thread[nT];
            for (int i = 0; i < nT; i++) {
                final long jan = startTsSec + tsOffsetSec + (i * minToWrite) * 60;
                tt[i] = new Thread() {
                    public void run() {
                        L2 ll2 = new L2();
                        ll2.writeValues(jan, minToWrite, 50000);
                    }
                };
            }

            long ts = System.currentTimeMillis();
            for (Thread t : tt) {
                t.start();
            }

            for (Thread t : tt) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            ts = System.currentTimeMillis() - ts;
            System.out.println("--------------------------------------------");
            System.out.println("N of Threads: " + nT);
            System.out.println("min to write: " + minToWrite);
            System.out.println("All Threads time ms: " + ts);
            System.out.printf("All Threads Write per sec: %.2f\n", nT * minToWrite * metricN
                    / (ts / 1000D));
            System.out.printf("total rows: %,d\n", nT * minToWrite * metricN);
        }
    }

    void writeValues(long startTime, int minutes, int batchSize) {
        Configuration conf = getConf();

        List<Integer> metricIds = new ArrayList<Integer>(metricN);
        for (int i = 0; i < metricN; i++) {
            metricIds.add(minMetric + i);
        }

        long t = System.currentTimeMillis();
        HTable t1 = null;
        long totalCnt = minutes * metricN;
        try {
            t1 = new HTable(conf, Bytes.toBytes("t1"));
            System.out.println("StartTime: " + startTime);
            long time = startTime - 60L;
            List<Put> putLi = new ArrayList<Put>(batchSize * 2);
            int batchCnt = 0;
            for (int i = 0; i < minutes; i++) {
                time += 60L;
                // load balance by shuffle metric ids
                Collections.shuffle(metricIds);
                for (int mId : metricIds) {
                    String s = "" + mId + time;
                    if (debug) {
                        System.out.println(s);
                    }
                    byte[] key = Bytes.toBytes(s);

                    int mi = rnd.nextInt(10000);
                    int ma = mi + rnd.nextInt(1000000);
                    byte[] minV = Bytes.toBytes(Integer.toString(mi));
                    byte[] maxV = Bytes.toBytes(Integer.toString(ma));

                    Put put = new Put(key);
                    put.add(F1, MIN, minV);
                    put.add(F1, MAX, maxV);
                    putLi.add(put);

                    if (putLi.size() == batchSize) {
                        put(t1, putLi, time);
                        batchCnt++;
                        printProgress(totalCnt, batchSize, batchCnt);
                    }
                }
            }
            if (putLi.size() > 0) {
                put(t1, putLi, time);
                printProgress(batchSize, batchSize, 1);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(t1);
        }
        t = System.currentTimeMillis() - t;
        System.out.printf("Total exec time sec: %.2f\n", t / 1000D);
        System.out.printf("Rows per sec: %.2f\n", totalCnt / (t / 1000D));
    }

    void put(HTable t1, List<Put> putLi, long ts) throws RetriesExhaustedWithDetailsException,
            InterruptedIOException {
        long t = System.currentTimeMillis();
        t1.put(putLi);
        t = System.currentTimeMillis() - t;
        System.out.println("ts: " + ts);
        System.out.println("time: " + t);
        putLi.clear();
    }

    void printProgress(long totalRows, int batchSize, int batchCnt) {
        double proc = batchSize * batchCnt / (totalRows / 100D);
        System.out.printf("progress: %.2f%%\n", proc);
    }

    HTable getHTable() throws IOException {
        Configuration conf = getConf();
        HTable t1 = new HTable(conf, Bytes.toBytes("t1"));
        return t1;
    }

    public void readSingleValue(String rowKey) {
        HTable t1 = null;
        try {
            t1 = getHTable();
            readValue(t1, rowKey);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(t1);
        }
    }

    void readValue(HTable t1, String rowKey) {
        try {
            Get get = new Get(Bytes.toBytes(rowKey));
            long t = System.currentTimeMillis();
            Result res = t1.get(get);
            t = System.currentTimeMillis() - t;
            String minV = Bytes.toString(res.getValue(F1, MIN));
            String maxV = Bytes.toString(res.getValue(F1, MAX));
            System.out.println("min: " + minV);
            System.out.println("max: " + maxV);
            System.out.println("time " + t);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    Configuration getConf() {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("lc/core-site.xml");
        conf.addResource("lc/hdfs-site.xml");
        conf.addResource("lc/hbase-site.xml");
        String defaultFs = conf.get("fs.defaultFS");
        String rootDir = conf.get("hbase.rootdir");
        System.out.println(defaultFs);
        System.out.println(rootDir);

        assert (defaultFs.startsWith("hdfs"));
        assert (rootDir.startsWith("hdfs"));
        return conf;
    }

    protected String getRandomKey() {
        int p1 = minMetric + rnd.nextInt(metricN);
        long p2 = startTsSec + rnd.nextInt(tsOffsetSec) * 60L;
        String key = "" + p1 + p2;
        return key;
    }

    public void readRandomValues(int N) {
        HTable t1 = null;
        try {
            t1 = getHTable();
            long maxGetTime = 0L;
            long minGetTime = Long.MAX_VALUE;
            long totalGetTime = 0L;
            long totalRunTime = System.currentTimeMillis();
            for (int i = 0; i < N; i++) {
                String k = getRandomKey();
                if (debug) {
                    System.out.println(k);
                }
                byte[] kBa = Bytes.toBytes(k);
                Get get = new Get(kBa);
                get.addFamily(F1);

                long t = System.currentTimeMillis();
                Result res = t1.get(get);
                t = System.currentTimeMillis() - t;
                totalGetTime += t;
                if (t > maxGetTime) {
                    maxGetTime = t;
                }

                if (t < minGetTime) {
                    minGetTime = t;
                }
                res.getValue(F1, MIN);
                res.getValue(F1, MAX);
                if (debug) {
                    String minV = Bytes.toString(res.getValue(F1, MIN));
                    String maxV = Bytes.toString(res.getValue(F1, MAX));
                    System.out.println("min: " + minV);
                    System.out.println("max: " + maxV);
                }
            }
            totalRunTime = System.currentTimeMillis() - totalRunTime;
            System.out.println("Min Get Time ms: " + minGetTime);
            System.out.println("Max Get Time ms: " + maxGetTime);
            System.out.printf("Avg Get Time ms: %.2f\n", totalGetTime * 1D / N);
            System.out.println("Total Get Time ms: " + totalGetTime);
            System.out.printf("Get per sec: %.2f\n", N / (totalGetTime / 1000D));
            System.out.println("Total Run Time ms: " + totalRunTime);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(t1);
        }
    }
}
