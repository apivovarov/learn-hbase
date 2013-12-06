package org.x4444.hbase.S01;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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

  public static void main(String[] args) {
    L2 l2 = new L2();
    l2.readValue();

    long jan = 1356998400 + 20100 * 60;
    //l2.writeValues(1000000, 100, jan, 50000);
  }

  void writeValues(int minutes, int metrics, long startTime, int batch) {
    Configuration conf = getConf();
    Random r = new Random();

    HTable t1 = null;
    try {
      t1 = new HTable(conf, Bytes.toBytes("t1"));

      long time = startTime - 60L;
      List<Put> putLi = new ArrayList<Put>(batch * 2);
      for (int i = 0; i < minutes; i++) {
        time += 60L;
        for (int j = 0; j < metrics; j++) {
          int mId = 50001 + j;
          // byte[] key = new byte[10];
          // byte[] mIdBa = Bytes.toBytes(mId);
          // byte[] timeBa = Bytes.toBytes(time);
          // Bytes.putShort(key, 0, mId);
          // Bytes.putShort(key, 2, time);
          String s = "" + mId + time;
          // System.out.println(s);
          byte[] key = Bytes.toBytes(s);

          int mi = r.nextInt(10000);
          int ma = mi + r.nextInt(1000000);
          byte[] minV = Bytes.toBytes(Integer.toString(mi));
          byte[] maxV = Bytes.toBytes(Integer.toString(ma));

          Put put = new Put(key);
          put.add(F1, MIN, minV);
          put.add(F1, MAX, maxV);
          putLi.add(put);
        }
        if (putLi.size() == batch) {
          put(t1, putLi, time);
        }
      }
      if (putLi.size() > 0) {
        put(t1, putLi, time);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (t1 != null) {
        try {
          t1.close();
        } catch (Exception e) {

        }
      }
    }
  }
  
  void put(HTable t1, List<Put> putLi, long ts) throws RetriesExhaustedWithDetailsException, InterruptedIOException {
    long t = System.currentTimeMillis();
    t1.put(putLi);
    t = System.currentTimeMillis() - t;
    System.out.println("ts: " + ts);
    System.out.println("time: " + t);
    putLi.clear();
  }

  void readValue() {
    Configuration conf = getConf();

    HTable t1 = null;
    try {
      t1 = new HTable(conf, Bytes.toBytes("t1"));

      String rowKey = "500451373768580";
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
    } finally {
      if (t1 != null) {
        try {
          t1.close();
        } catch (Exception e) {

        }
      }
    }
  }

  Configuration getConf() {
    Configuration conf = HBaseConfiguration.create();
    conf.addResource("core-site.xml");
    conf.addResource("hbase-site.xml");
    String defaultFs = conf.get("fs.defaultFS");
    String rootDir = conf.get("hbase.rootdir");
    System.out.println(defaultFs);
    System.out.println(rootDir);

    assert (defaultFs.startsWith("hdfs"));
    assert (rootDir.startsWith("hdfs"));
    return conf;
  }
}
