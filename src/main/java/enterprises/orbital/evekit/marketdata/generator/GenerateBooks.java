package enterprises.orbital.evekit.marketdata.generator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.base.PersistentProperty;
import enterprises.orbital.db.DBPropertyProvider;
import enterprises.orbital.evekit.marketdata.model.EveKitMarketDataProvider;
import enterprises.orbital.evekit.marketdata.model.Instrument;
import enterprises.orbital.evekit.marketdata.model.Order;
import enterprises.orbital.evekit.marketdata.model.Region;

/**
 * Build interval files from region files for a given type. A region file is a file with path/name:
 * 
 * <pre>
 * regions/&lt;regionID&gt;/region_&lt;snapTime&gt;_&lt;date&gt;.gz
 * </pre>
 * 
 * The format of a region file is:
 *
 * <pre>
 * total number of orders
 * order 1
 * order 2
 * ...
 * order N
 * </pre>
 * 
 * where each order has form:
 * 
 * <pre>
 * regionID,typeID,orderID,buy,issued,price,volumeEntered,minVolume,volume,orderRange,locationID,duration
 * </pre>
 * 
 * Region files are parsed into interval files. An interval file is a file with path:
 * 
 * <pre>
 * &lt;year&gt;/&lt;month&gt;/&lt;day&gt;/&lt;prefix&gt;_&lt;typeID&gt;_&lt;date&gt;_&lt;interval_in_minutes&gt;.book
 * </pre>
 * 
 * Each interval file has the format:
 * 
 * <pre>
 * typeID
 * snapshots per region
 * first regionID
 * first region first snapshot time in milliseconds UTC
 * first region first snapshot Number of Buy orders
 * first region first snapshot Number of Sell orders
 * first region first snapshot Buy orders, one per line, highest price first
 * first region first snapshot Sell orders, one per line, lowest price first
 * first region second snapshot...
 * second regionID
 * ...
 * </pre>
 * 
 * Each buy or sell order is output on a single line as the following comma separated fields:
 * 
 * <pre>
 * orderID,buy,issued,price,volumeEntered,minVolume,volume,orderRange,locationID,duration
 * </pre>
 * 
 * where "issued" is the order issue time in milliseconds UTC and "price" is a floating value to two decimal places.
 *
 */
public class GenerateBooks {
  // Location where region snapshots are stored in the format regions/<regionID>/region_<snapTime>_<date>.gz
  public static final String         PROP_REGION_DIR        = "enterprises.orbital.evekit.marketdata.regionDir";
  public static final String         DFLT_REGION_DIR        = "";
  // Number of books to generate in parallel on each thread
  public static final String         PROP_BOOKS_IN_PARALLEL = "enterprises.orbital.evekit.marketdata.booksInParallel";
  public static final int            DFLT_BOOKS_IN_PARALLEL = 10;
  // Sort bids from highest price to lowest
  protected static Comparator<Order> bidComparator          = new Comparator<Order>() {

                                                              @Override
                                                              public int compare(
                                                                                 Order o1,
                                                                                 Order o2) {
                                                                // Sort bids highest prices first
                                                                return -o1.getPrice().compareTo(o2.getPrice());
                                                              }

                                                            };
  // Sort asks from lowest price to highest
  protected static Comparator<Order> askComparator          = new Comparator<Order>() {

                                                              @Override
                                                              public int compare(
                                                                                 Order o1,
                                                                                 Order o2) {
                                                                // Sort asks lowest prices first
                                                                return o1.getPrice().compareTo(o2.getPrice());
                                                              }

                                                            };

  // Data structure representing the book for a type at a particular time
  protected static class InstrumentBook {
    public int        typeID;
    public long       snapTime;
    // Sorted largest price first
    public Set<Order> bid = new TreeSet<>(bidComparator);
    // Sorted lowest price first
    public Set<Order> ask = new TreeSet<>(askComparator);

    public InstrumentBook(int typeID, long snapTime) {
      super();
      this.typeID = typeID;
      this.snapTime = snapTime;
    }
  }

  protected static class DumpRequest {
    public int[]  typeSet;
    public Date   day;
    public File   outputDir;
    public String prefix;
    public long   intervals;

    public DumpRequest(int[] typeSet, Date day, File outputDir, String prefix, long intervals) {
      super();
      this.typeSet = typeSet;
      this.day = day;
      this.outputDir = outputDir;
      this.prefix = prefix;
      this.intervals = intervals;
    }
  }

  protected static class DumpRequestHandler implements Runnable {
    protected DumpRequest next;

    public DumpRequestHandler(DumpRequest n) {
      next = n;
    }

    @Override
    public void run() {
      SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
      formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
      String printDay = formatter.format(next.day);
      try {
        dumpRegionsDay(next.typeSet, next.day, next.outputDir, next.prefix, next.intervals);
        System.out.println(String.format("Generated ({%s}, %s)", Arrays.toString(next.typeSet), printDay));
      } catch (Exception e) {
        System.err.println(String.format("Failed generation of ({%s}, %s): %s", Arrays.toString(next.typeSet), printDay, e.toString()));
        e.printStackTrace(System.err);
        System.exit(1);
      }
    }
  }

  // Types to iterate over - pulled from DB
  protected static Set<Integer> typeSet   = new HashSet<Integer>();
  // Regions to iterate over - pulled from DB
  protected static Set<Integer> regionSet = new HashSet<Integer>();

  protected static void buildTypeSet() throws IOException, ExecutionException {
    typeSet.addAll(Instrument.getActiveTypeIDs());
  }

  protected static void buildRegionSet() throws IOException, ExecutionException {
    regionSet.addAll(Region.getActiveRegionIDs());
  }

  protected static void usage() {
    System.err.println("Usage: GenerateBooks [-h] [-d <dir>] [-i intervalSizeInMin] [-w YYYY-MM-DD] [-p prefix] [-t booksPerCycle] [-m typeid]");
    System.exit(0);
  }

  public static void main(
                          String[] argv)
    throws Exception {
    OrbitalProperties.addPropertyFile("EveKitMarketdata.properties");
    PersistentProperty.setProvider(new DBPropertyProvider(OrbitalProperties.getGlobalProperty(EveKitMarketDataProvider.PROP_MARKETDATA_PU)));
    buildTypeSet();
    buildRegionSet();
    Date defaultDate = Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime();
    String defaultDirectory = ".";
    long defaultInterval = TimeUnit.MINUTES.convert(1, TimeUnit.HOURS);
    String defaultPrefix = "interval";
    int defaultThreads = 1;
    Set<Integer> types = new HashSet<Integer>();
    for (int i = 0; i < argv.length; i++) {
      if (argv[i].equals("-h"))
        usage();
      else if (argv[i].equals("-m")) {
        if (i + 1 == argv.length) usage();
        i++;
        types.add(Integer.valueOf(argv[i]));
      } else if (argv[i].equals("-t")) {
        if (i + 1 == argv.length) usage();
        i++;
        defaultThreads = Math.min(100, Math.max(1, Integer.valueOf(argv[i])));
      } else if (argv[i].equals("-d")) {
        if (i + 1 == argv.length) usage();
        i++;
        defaultDirectory = argv[i];
      } else if (argv[i].equals("-i")) {
        if (i + 1 == argv.length) usage();
        i++;
        defaultInterval = Long.valueOf(argv[i]);
      } else if (argv[i].equals("-p")) {
        if (i + 1 == argv.length) usage();
        i++;
        defaultPrefix = argv[i];
      } else if (argv[i].equals("-w")) {
        if (i + 1 == argv.length) usage();
        i++;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        defaultDate = formatter.parse(argv[i]);
      } else
        usage();
    }
    dumpRegions(defaultDate, new File(defaultDirectory), defaultPrefix, defaultInterval, defaultThreads, types);
  }

  /**
   * Dump all order books for all regions for the given day.
   * 
   * @param day
   *          day to output
   * @param outputDir
   *          directory where book files will be written
   * @param prefix
   *          prefix to prepend to book file names
   * @param interval
   *          interval size in minutes (min: 5 min, max: 24 hours)
   * @param threads
   *          the number of threads to use for writing output
   * @param typeID
   *          the set of types to dump
   * @throws IOException
   *           on SQL or IO error
   * @throws InterruptedException
   *           if interrupted while waiting for work
   */
  public static void dumpRegions(
                                 Date day,
                                 File outputDir,
                                 String prefix,
                                 long interval,
                                 int threads,
                                 Set<Integer> typeID)
    throws IOException, InterruptedException {
    if (typeID.isEmpty()) typeID.addAll(typeSet);
    ForkJoinPool executor = new ForkJoinPool(threads);
    System.out.println(String.format("Dumping books for %d types", typeID.size()));
    int p = (int) OrbitalProperties.getLongGlobalProperty(PROP_BOOKS_IN_PARALLEL, DFLT_BOOKS_IN_PARALLEL);
    Integer[] flattened = typeID.toArray(new Integer[typeID.size()]);
    for (int i = 0; i < flattened.length; i += p) {
      int[] nextSet = new int[Math.min(p, flattened.length - i)];
      for (int j = 0; j < nextSet.length; j++)
        nextSet[j] = flattened[i + j];
      executor.submit(new DumpRequestHandler(new DumpRequest(nextSet, day, outputDir, prefix, interval)));
    }
    // Wait until all tasks are complete
    executor.shutdown();
    if (executor.awaitTermination(6, TimeUnit.HOURS)) {
      System.out.println("Execution completed normally");
    } else {
      System.out.println("Executor failed to complete before timeout, shutting down");
    }
  }

  /**
   * Dump the book for the given region and type on the given day. The output is written to a file with name:
   * &lt;outputDir&gt;/&lt;prefix&gt;_&lt;typeID&gt;_&lt;YYYYMMDD&gt;_&lt;interval&gt;.book.gz The provided date is interpreted as UTC, any time value is
   * ignored.
   * 
   * @param typeSet
   *          Array of types to dump in parallel. This is usually more efficient if memory is available as it avoids having to rescan region files multiple
   *          times.
   * @param day
   *          day to dump (see notes above)
   * @param outputDir
   *          output directory where book file will be written
   * @param prefix
   *          prefix for output file name
   * @param intervals
   *          minutes between each book snapshot. Maximum is 1440 (24 hours). Minimum is 5.
   * @throws IOException
   *           if error occurs writing book file
   */
  public static void dumpRegionsDay(
                                    int[] typeSet,
                                    Date day,
                                    File outputDir,
                                    String prefix,
                                    long intervals)
    throws IOException {
    // Populate set for faster membership checks below
    Set<Integer> typeFilter = new HashSet<Integer>();
    for (int i = 0; i < typeSet.length; i++)
      typeFilter.add(typeSet[i]);
    // Sanity check intervals
    intervals = Math.min(intervals, TimeUnit.MINUTES.convert(24, TimeUnit.HOURS));
    intervals = Math.max(intervals, 5);
    // Determine book output boundaries (00:00 UTC to 24:00 UTC)
    final long millisPerDay = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
    long startTime = (day.getTime() / millisPerDay) * millisPerDay;
    long start = startTime;
    long end = start + TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
    // Prepare output files
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    final String targetDate = formatter.format(new Date(startTime));
    String[] fileName = new String[typeSet.length];
    File[] targetFile = new File[typeSet.length];
    PrintWriter[] bookFile = new PrintWriter[typeSet.length];
    for (int i = 0; i < typeSet.length; i++) {
      fileName[i] = String.format("%s_%d_%s_%d.book.gz", prefix, typeSet[i], targetDate, intervals);
      targetFile[i] = new File(outputDir, fileName[i]);
      targetFile[i].getParentFile().mkdirs();
      bookFile[i] = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(targetFile[i]))));
    }
    // Move intervals to milliseconds
    intervals = TimeUnit.MILLISECONDS.convert(intervals, TimeUnit.MINUTES);
    try {
      // Write header
      int totalIntervalCount = (int) (TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS) / intervals);
      for (int i = 0; i < typeSet.length; i++) {
        bookFile[i].format("%d\n%d\n", typeSet[i], totalIntervalCount);
      }
      // Iterate over all regions - some regions may be empty
      String regionDir = OrbitalProperties.getGlobalProperty(PROP_REGION_DIR, DFLT_REGION_DIR) + File.separator + "regions";
      for (int regionID : regionSet) {
        // For each region:
        // 1. Assemble all of the available region files for the given day.
        String thisRegionDir = regionDir + File.separator + String.valueOf(regionID);
        File regionDirFile = new File(thisRegionDir);
        List<File> regionFiles = new ArrayList<>();
        for (File nextRegionFile : regionDirFile.listFiles(new FilenameFilter() {

          @Override
          public boolean accept(
                                File dir,
                                String name) {
            return name.endsWith("_" + targetDate + ".gz");
          }

        })) {
          regionFiles.add(nextRegionFile);
        }
        // 2. Include the last file for the region from the previous day if available.
        long previousDayStart = startTime - TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
        final String prevDate = formatter.format(new Date(previousDayStart));
        File[] lastDayFiles = regionDirFile.listFiles(new FilenameFilter() {

          @Override
          public boolean accept(
                                File dir,
                                String name) {
            return name.endsWith("_" + prevDate + ".gz");
          }

        });
        Arrays.sort(lastDayFiles, new Comparator<File>() {

          @Override
          public int compare(
                             File arg0,
                             File arg1) {
            String arg0Name = arg0.getName();
            String arg1Name = arg1.getName();
            long arg0Time = Long.valueOf((arg0Name.split("_"))[1]);
            long arg1Time = Long.valueOf((arg1Name.split("_"))[1]);
            if (arg0Time < arg1Time) return -1;
            if (arg0Time > arg1Time) return 1;
            return 0;
          }
        });
        if (lastDayFiles.length > 0) regionFiles.add(lastDayFiles[lastDayFiles.length - 1]);
        // 3. Read each available region file and build the book for the given type at the given time.
        Map<Integer, List<Order>> orderList = new HashMap<Integer, List<Order>>();
        Map<Integer, SortedMap<Long, InstrumentBook>> booksForDay = new HashMap<Integer, SortedMap<Long, InstrumentBook>>();
        for (int i = 0; i < typeSet.length; i++) {
          orderList.put(typeSet[i], new ArrayList<Order>());
          booksForDay.put(typeSet[i], new TreeMap<Long, InstrumentBook>());
        }
        for (File next : regionFiles) {
          try (LineNumberReader reader = new LineNumberReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(next))))) {
            long snapTime = Long.valueOf((next.getName().split("_"))[1]);
            int orderCount = Integer.valueOf(reader.readLine());
            for (int i = 0; i < orderCount; i++) {
              String nextOrderLine = reader.readLine();
              String[] values = nextOrderLine.split(",");
              assert Integer.valueOf(values[0]).intValue() == regionID;
              int typeID = Integer.valueOf(values[1]);
              // Filter if this order is for a different type
              if (!typeFilter.contains(typeID)) continue;
              long orderID = Long.valueOf(values[2]);
              boolean buy = Boolean.valueOf(values[3]);
              long issued = Long.valueOf(values[4]);
              BigDecimal price = BigDecimal.valueOf(Double.valueOf(values[5]).doubleValue()).setScale(2, RoundingMode.HALF_UP);
              int volumeEntered = Integer.valueOf(values[6]);
              int minVolume = Integer.valueOf(values[7]);
              int volume = Integer.valueOf(values[8]);
              String orderRange = values[9];
              long locationID = Long.valueOf(values[10]);
              int duration = Integer.valueOf(values[11]);
              // Save order
              orderList.get(typeID)
                  .add(new Order(regionID, typeID, orderID, buy, issued, price, volumeEntered, minVolume, volume, orderRange, locationID, duration));
            }
            for (int i = 0; i < typeSet.length; i++) {
              int typeID = typeSet[i];
              if (!orderList.get(typeID).isEmpty()) {
                // We have orders, save this book.
                InstrumentBook newBook = new InstrumentBook(typeID, snapTime);
                for (Order nextOrder : orderList.get(typeID)) {
                  if (nextOrder.isBuy())
                    newBook.bid.add(nextOrder);
                  else
                    newBook.ask.add(nextOrder);
                }
                booksForDay.get(typeID).put(snapTime, newBook);
              }
            }
          } finally {
            // Clear order list in prep for next region
            for (int i = 0; i < typeSet.length; i++)
              orderList.get(typeSet[i]).clear();
          }
        }
        // 4. Iterate through the intervals for the day and output the book that was current as of the interval time
        for (int i = 0; i < typeSet.length; i++) {
          int typeID = typeSet[i];
          bookFile[i].format("%d\n", regionID);
          for (start = startTime; start < end; start += intervals) {
            // Dump current book at this time
            try {
              dumpBookAtTime(start, bookFile[i], booksForDay.get(typeID));
            } catch (IOException e) {
              String errMsg = String.format("Failed to write (%d, %d) at %d, skipping this interval (exception follows)", regionID, typeID, start);
              System.err.println(errMsg);
              e.printStackTrace(System.err);
            }
          }
        }
        // 5. Done with this region
      }
    } finally {
      for (int i = 0; i < typeSet.length; i++)
        bookFile[i].close();
    }
  }

  protected static void writeOrder(
                                   Order o,
                                   PrintWriter out) {
    out.format("%d,%b,%d,%.2f,%d,%d,%d,%s,%d,%d\n", o.getOrderID(), o.isBuy(), o.getIssued(), o.getPrice(), o.getVolumeEntered(), o.getMinVolume(),
               o.getVolume(), o.getOrderRange(), o.getLocationID(), o.getDuration());
  }

  protected static void dumpBookAtTime(
                                       long asOf,
                                       PrintWriter out,
                                       SortedMap<Long, InstrumentBook> books)
    throws IOException {
    // Find entry closest to target time without going over
    SortedMap<Long, InstrumentBook> bookAtTime = books.headMap(asOf + 1);
    if (bookAtTime.isEmpty()) {
      // If no entry, then write default header and return
      out.format("%d\n%d\n%d\n", asOf, 0, 0);
    } else {
      // Write time header
      out.format("%d\n", asOf);
      // Last book in map needs to be written out
      InstrumentBook last = bookAtTime.get(bookAtTime.lastKey());
      // ZipEntry is already in correct format. Just need to copy to output
      out.format("%d\n", last.bid.size());
      out.format("%d\n", last.ask.size());
      for (Order next : last.bid)
        writeOrder(next, out);
      for (Order next : last.ask)
        writeOrder(next, out);
    }
  }
}
