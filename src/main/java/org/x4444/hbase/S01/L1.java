package org.x4444.hbase.S01;

import org.apache.hadoop.hbase.util.Bytes;
import org.x4444.hbase.X4Bytes;

public class L1 {

  public static byte[] getKey(int metricId, int boxId, long ts) {
    checkShortRange(boxId);
    checkShortRange(metricId);
    byte[] keyB = new byte[12];

    Bytes.putShort(keyB, 0, (short) metricId);
    printBytes(keyB);

    Bytes.putShort(keyB, 2, (short) boxId);
    printBytes(keyB);

    System.out.println(ts);
    Bytes.putLong(keyB, 4, ts);

    Bytes.toBytes(1L);
    printBytes(keyB);
    return keyB;
  }

  static void printBytes(byte[] bytes) {
    if (bytes.length == 6) {
      System.out.print("    ");
    }
    for (byte b : bytes) {
      System.out.print(b + ",");
    }
    System.out.println();
  }

  static void compareByte6ToByte8(byte[] b6, byte[] b8) {
    for (int i = 0; i < 6; i++) {
      if (b6[i] != b8[i + 2]) {
        throw new IllegalArgumentException("i: " + i + " b6 <> b8");
      }
    }
  }

  public static void checkShortRange(int v) {
    if (v < Short.MIN_VALUE) {
      throw new IllegalArgumentException("min v is " + Short.MIN_VALUE);
    }
    if (v > Short.MAX_VALUE) {
      throw new IllegalArgumentException("max v is " + Short.MAX_VALUE);
    }
  }

  public static void main(String[] args) {
    long ts = (long) -Math.pow(2, 10);
    System.out.println(ts);
    // byte[] keyB = getKey(258, 21, ts);

    byte[] b6 = org.x4444.hbase.X4Bytes.toSixBytes(ts);
    printBytes(b6);
    byte[] b8 = Bytes.toBytes(ts);
    printBytes(b8);
    long t8 = Bytes.toLong(b8);
    long t6 = X4Bytes.sixBytesToLong(b6, 0);
    System.out.println(ts == t8);
    System.out.println(ts == t6);

    byte[] bb = new byte[] { 127, -1, -1, -1, -1, -1 };
    long bbL = X4Bytes.sixBytesToLong(bb, 0);
    System.out.println(t8);
    System.out.println(t6);
    System.out.println(bbL);

    testAll();
  }

  static void testAll() {
    Thread t1 = new Thread() {
      public void run() {
        long min = -140737488355328L;
        long max = -70000000000000L;
        doTest("t1", min, max, 138011L);
      };
    };

    Thread t2 = new Thread() {
      public void run() {
        long min = -70000000000000L;
        long max = 0;
        doTest("t2", min, max, 138013L);
      };
    };

    Thread t3 = new Thread() {
      public void run() {
        long min = 0;
        long max = 70000000000000L;
        doTest("t3", min, max, 138015L);
      };
    };

    Thread t4 = new Thread() {
      public void run() {
        long min = 70000000000000L;
        long max = 140737488355327L;
        doTest("t4", min, max, 138017L);
      };
    };
    long time = System.currentTimeMillis();
    t1.start();
    t2.start();
    t3.start();
    t4.start();

    try {
      t1.join();
      t2.join();
      t3.join();
      t4.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    time = System.currentTimeMillis() - time;
    System.out.format("time: %.2f sec%n", time / 1000.0D);
  }

  static void doTest(String n, long min, long max, long inc) {
    System.out.println(n + " testing...");
    byte bb = 0;
    long ts = min;
    boolean firstTime = true;
    boolean stop = false;
    while (!stop) {
      if (!firstTime) {
        ts += inc;
      } else {
        firstTime = false;
      }
      if (ts > max) {
        ts = max;
        stop = true;
      }
      byte[] b6 = X4Bytes.toSixBytes(ts);
      byte[] b8 = Bytes.toBytes(ts);

      if (b6[0] != bb) {
        bb = b6[0];
        System.out.println(n + ":" + ts);
      }
      compareByte6ToByte8(b6, b8);

      long t8 = Bytes.toLong(b8);
      long t6 = X4Bytes.sixBytesToLong(b6, 0);

      if (t8 != t6) {
        throw new IllegalArgumentException("ts: " + ts + " t8 <> t6");
      }
    }
    System.out.println(n + " test done " + ts);
  }

  
}
