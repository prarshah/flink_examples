package _6_stateBackend;

import org.rocksdb.*;

public class _2_
{
    static {
        RocksDB.loadLibrary();
    }
public static void main(String args[])
{
    final String db_path = "/home/exa00105/PS/";
    System.out.println("RocksDBSample");

    try {final Options options = new Options();
         final Filter bloomFilter = new BloomFilter(10);
         final ReadOptions readOptions = new ReadOptions()
                 .setFillCache(false);
         final Statistics stats = new Statistics();
         final RateLimiter rateLimiter = new RateLimiter(10000000,10000, 10);

         System.out.println("part1 successful");
         //open
        options.setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, db_path);
        db.put("hello".getBytes(), "world".getBytes());

        System.out.println("part2 successful");

        final byte[] value = db.get("hello".getBytes());
        System.out.println(new String(value));

    }
    catch(Exception e)
    {e.printStackTrace();}

}

}
