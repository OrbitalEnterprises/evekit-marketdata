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
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.evekit.marketdata.model.MarketHistory;

/**
 * Convert market history snapshots into market history files. A market history file is a file with name
 * history_&lt;snapTime&gt;_&lt;region&gt;_&lt;date&gt;.gz. The file consists of a single line giving the number of history entries in the file, followed by one
 * line for each market history entry. All entries in the file are for the same type and region. Each market history line has format:
 * 
 * <pre>
 * typeID
 * regionID
 * orderCount
 * lowPrice
 * highPrice
 * avgPrice
 * volume
 * date in milliseconds UTC
 * </pre>
 *
 * A market history file, by default, is stored at "year"/"month"/"day"/market_"typeid"_"date".history.gz
 * 
 * The history dumper works slightly differently than the book dumper. Because several days of history are gathered in a single day's snapshots, the history
 * dumper takes responsibility for creating year/month/day prefixes as needed based on the coverage of the available snapshots.
 */
public class GenerateMarketHistory {
  // Location where history snapshots are stored in the format history/<typeid>/history_<snapTime>_<regionid>_<day>.gz
  public static final String PROP_HISTORY_DIR = "enterprises.orbital.evekit.marketdata.historyDir";
  public static final String DFLT_HISTORY_DIR = "";

  protected static class DumpRequest {
    public int    typeID;
    public Date   day;
    public Date   limit;
    public File   outputDir;
    public String prefix;

    public DumpRequest(int typeID, Date day, Date limit, File outputDir, String prefix) {
      super();
      this.typeID = typeID;
      this.day = day;
      this.limit = limit;
      this.outputDir = outputDir;
      this.prefix = prefix;
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
        dumpHistoryDay(next.typeID, next.day, next.limit, next.outputDir, next.prefix);
        System.out.println(String.format("Generated (%d, %s)", next.typeID, printDay));
      } catch (Exception e) {
        System.err.println(String.format("Failed generation of (%d, %s): %s", next.typeID, printDay, e.toString()));
        e.printStackTrace(System.err);
        System.exit(1);
      }
    }
  }

  protected static void usage() {
    System.err.println("Usage: GenerateMarketHistory [-h] [-d <dir>] [-w YYYY-MM-DD] [-p prefix] [-t threads] [-m typeid] [-l YYYY-MM-DD]");
    System.exit(0);
  }

  public static void main(
                          String[] argv)
    throws Exception {
    OrbitalProperties.addPropertyFile("EveKitMarketdata.properties");
    Date defaultDate = Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime();
    Date limitDate = new Date(Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime().getTime() - TimeUnit.MILLISECONDS.convert(700, TimeUnit.DAYS));
    String defaultDirectory = ".";
    String defaultPrefix = "market";
    int defaultThreads = 1;
    List<Integer> typeList = new ArrayList<Integer>();
    for (int i = 0; i < argv.length; i++) {
      if (argv[i].equals("-h"))
        usage();
      else if (argv[i].equals("-m")) {
        if (i + 1 == argv.length) usage();
        i++;
        typeList.add(Integer.valueOf(argv[i]));
      } else if (argv[i].equals("-t")) {
        if (i + 1 == argv.length) usage();
        i++;
        defaultThreads = Math.min(100, Math.max(1, Integer.valueOf(argv[i])));
      } else if (argv[i].equals("-d")) {
        if (i + 1 == argv.length) usage();
        i++;
        defaultDirectory = argv[i];
      } else if (argv[i].equals("-p")) {
        if (i + 1 == argv.length) usage();
        i++;
        defaultPrefix = argv[i];
      } else if (argv[i].equals("-w")) {
        if (i + 1 == argv.length) usage();
        i++;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        defaultDate = formatter.parse(argv[i]);
      } else if (argv[i].equals("-l")) {
        if (i + 1 == argv.length) usage();
        i++;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        limitDate = formatter.parse(argv[i]);
      } else
        usage();
    }
    dumpHistory(defaultDate, limitDate, new File(defaultDirectory), defaultPrefix, defaultThreads, typeList);
  }

  /**
   * Dump all market history snapshots recorded for the given day.
   * 
   * @param day
   *          day on which all snapshots were recorded
   * @param limit
   *          day before which market history will not be produced
   * @param outputDir
   *          directory where history files will be written
   * @param prefix
   *          prefix to prepend to history file names
   * @param threads
   *          the number of threads to use for writing output
   * @param typeList
   *          if non-empty, then only dump market history for the listed types
   * @throws IOException
   *           on SQL or IO error
   * @throws InterruptedException
   *           if interrupted while waiting for work
   */
  public static void dumpHistory(
                                 Date day,
                                 Date limit,
                                 File outputDir,
                                 String prefix,
                                 int threads,
                                 List<Integer> typeList)
    throws IOException, InterruptedException {
    if (typeList.isEmpty()) {
      // Retrieve and add all available types
      File historyDir = new File(OrbitalProperties.getGlobalProperty(PROP_HISTORY_DIR, DFLT_HISTORY_DIR) + File.separator + "history");
      for (String fn : historyDir.list()) {
        try {
          int nextTypeID = Integer.valueOf(fn);
          typeList.add(nextTypeID);
        } catch (NumberFormatException e) {
          // ignore
        }
      }
    }
    ForkJoinPool executor = new ForkJoinPool(threads);
    System.out.println(String.format("Dumping history for %d types", typeList.size()));
    for (int nextType : typeList) {
      executor.submit(new DumpRequestHandler(new DumpRequest(nextType, day, limit, outputDir, prefix)));
    }
    // Wait until all tasks are complete
    executor.shutdown();
    executor.awaitTermination(6, TimeUnit.HOURS);
  }

  /**
   * Dump the history for the given type from all snapshots recorded on the given day. The provided date is interpreted as UTC, any time value is ignored.
   * 
   * @param typeID
   *          type to dump
   * @param day
   *          day to dump
   * @param limit
   *          day before which snapshots will not be generated
   * @param outputDir
   *          output directory where history file will be written
   * @param prefix
   *          prefix for output file name
   * @throws IOException
   *           if error occurs writing history file
   */
  protected static void dumpHistoryDay(
                                       int typeID,
                                       Date day,
                                       Date limit,
                                       File outputDir,
                                       String prefix)
    throws IOException {
    // Determine history output time (00:00 UTC)
    final long millisPerDay = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
    long snapTime = (day.getTime() / millisPerDay) * millisPerDay;
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    final String snapDate = formatter.format(new Date(snapTime));
    // Iterate over available regions for this type
    String historyDir = OrbitalProperties.getGlobalProperty(PROP_HISTORY_DIR, DFLT_HISTORY_DIR) + File.separator + "history" + File.separator
        + String.valueOf(typeID);
    File historyDirFile = new File(historyDir);
    // Extract the set of history files to process for the snap date.
    // If a region appears more than once, then use the later file.
    Map<Integer, File> regionFiles = new HashMap<>();
    for (File nextRegion : historyDirFile.listFiles(new FilenameFilter() {

      @Override
      public boolean accept(
                            File dir,
                            String name) {
        return name.endsWith("_" + snapDate + ".gz");
      }
    })) {
      String[] fields = nextRegion.getName().split("_");
      int regionID = Integer.valueOf(fields[2]);
      long fileSnapTime = Long.valueOf(fields[1]);
      if (regionFiles.containsKey(regionID)) {
        // Check whether current file is older
        File current = regionFiles.get(regionID);
        fields = current.getName().split("_");
        long currentSnapTime = Long.valueOf(fields[1]);
        if (fileSnapTime > currentSnapTime) {
          // replace
          regionFiles.put(regionID, nextRegion);
        }
      } else {
        regionFiles.put(regionID, nextRegion);
      }
    }
    // Process region files
    for (File nextRegion : regionFiles.values()) {
      String[] fields = nextRegion.getName().split("_");
      int regionID = Integer.valueOf(fields[2]);
      try (LineNumberReader reader = new LineNumberReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(nextRegion))))) {
        // Extract all market history records contained in this file
        int historyCount = Integer.valueOf(reader.readLine());
        for (int i = 0; i < historyCount; i++) {
          String nextLine = reader.readLine();
          String[] values = nextLine.split(",");
          int orderCount = Integer.valueOf(values[2]);
          BigDecimal lowPrice = BigDecimal.valueOf(Double.valueOf(values[3]).doubleValue()).setScale(2, RoundingMode.HALF_UP);
          BigDecimal highPrice = BigDecimal.valueOf(Double.valueOf(values[4]).doubleValue()).setScale(2, RoundingMode.HALF_UP);
          BigDecimal avgPrice = BigDecimal.valueOf(Double.valueOf(values[5]).doubleValue()).setScale(2, RoundingMode.HALF_UP);
          long volume = Long.valueOf(values[6]);
          long date = Long.valueOf(values[7]);
          if (date <= limit.getTime())
            // Skip dates before the limit date
            continue;
          MarketHistory result = new MarketHistory(typeID, regionID, orderCount, lowPrice, highPrice, avgPrice, volume, date);
          // Prepare to store entry
          String historyDate = formatter.format(new Date(date));
          String hYear = historyDate.substring(0, 4);
          String hMonth = historyDate.substring(4, 6);
          String hDay = historyDate.substring(6, 8);
          // Create or append to file for this market history day
          String fileName = String.format("%s_%d_%s.history.gz", prefix, typeID, historyDate);
          File targetFile = new File(new File(new File(new File(outputDir, hYear), hMonth), hDay), fileName);
          targetFile.getParentFile().mkdirs();
          try (PrintWriter historyFile = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(targetFile, true))))) {
            writeHistory(result, historyFile);
          }
        }
      }
    }
  }

  protected static void writeHistory(
                                     MarketHistory h,
                                     PrintWriter out) {
    out.format("%d,%d,%d,%.2f,%.2f,%.2f,%d,%d\n", h.getTypeID(), h.getRegionID(), h.getOrderCount(), h.getLowPrice(), h.getHighPrice(), h.getAvgPrice(),
               h.getVolume(), h.getDate());
  }

}
