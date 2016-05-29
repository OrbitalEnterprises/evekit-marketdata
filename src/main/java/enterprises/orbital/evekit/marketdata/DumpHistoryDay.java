package enterprises.orbital.evekit.marketdata;

import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import enterprises.orbital.base.OrbitalProperties;

/**
 * Convert market history snapshots into market history files. A history file consists of one or more market snapshot histories, one for each region, where each
 * line has the following format:
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
 * A market history file, by default, is stored at "year"/"month"/"day"/market_"typeid"_"date".history
 * 
 * The history dumper works slightly differently than the book dumper. Because several days of history are gathered in a single day's snapshots, the history
 * dumper takes responsibility for creating year/month/day prefixes as needed based on the coverage of the available snapshots.
 */
public class DumpHistoryDay {
  // Location where history snapshots are stored in the format history/<typeid>/history_<regionid>_<day>.zip
  public static final String PROP_HISTORY_DIR = "enterprises.orbital.evekit.marketdata.historyDir";
  public static final String DEF_HISTORY_DIR  = "";

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
    System.err.println("Usage: DumpHistoryDay [-h] [-d <dir>] [-w YYYY-MM-DD] [-p prefix] [-t threads] [-m typeid] [-l YYYY-MM-DD]");
    System.exit(0);
  }

  public static void main(
                          String[] argv)
    throws Exception {
    OrbitalProperties.addPropertyFile("EveKitMarketdataScheduler.properties");
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

  public static void dumpHistory(
                                 String day,
                                 String limitDay,
                                 File outputDir,
                                 String prefix,
                                 int threads,
                                 List<Integer> typeList)
    throws IOException, ExecutionException, ParseException, InterruptedException {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    dumpHistory(formatter.parse(day), formatter.parse(limitDay), outputDir, prefix, threads, typeList);
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
      File historyDir = new File(OrbitalProperties.getGlobalProperty(PROP_HISTORY_DIR, DEF_HISTORY_DIR) + File.separator + "history");
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
   * Convenience version of dumpHistoryDay. @see DumpHistoryDay#dumpHistoryDay(int, int, Date, File, String)
   * 
   * @param typeID
   *          type to dump
   * @param day
   *          text string (format: YYYY-MM-DD) giving day to dump.
   * @param limit
   *          text string (format: YYYY-MM-DD) giving the day before which market history will not be dumped.
   * @param outputDir
   *          output directory where history file will be written
   * @param prefix
   *          prefix for output file name
   * @throws ParseException
   *           if date not parseable
   * @throws IOException
   *           on error writing book file
   */
  public static void dumpHistoryDay(
                                    int typeID,
                                    String day,
                                    String limit,
                                    File outputDir,
                                    String prefix)
    throws ParseException, IOException {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    dumpHistoryDay(typeID, formatter.parse(day), formatter.parse(limit), outputDir, prefix);
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
  public static void dumpHistoryDay(
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
    String historyDir = OrbitalProperties.getGlobalProperty(PROP_HISTORY_DIR, DEF_HISTORY_DIR) + File.separator + "history" + File.separator
        + String.valueOf(typeID);
    File historyDirFile = new File(historyDir);
    for (File nextRegion : historyDirFile.listFiles(new FilenameFilter() {

      @Override
      public boolean accept(
                            File dir,
                            String name) {
        return name.endsWith("_" + snapDate + ".zip");
      }
    })) {
      ZipFile regionZip = new ZipFile(nextRegion);
      String regStr = nextRegion.getName().substring("history_".length());
      int regionID = Integer.valueOf(regStr.substring(0, regStr.indexOf('_')));
      try {
        // Each region file consists of one or more day history snapshots already in the appropriate format. Iterate through these snapshots and append to the
        // appropriate history file.
        Enumeration<? extends ZipEntry> entries = regionZip.entries();
        while (entries.hasMoreElements()) {
          ZipEntry nextEntry = entries.nextElement();
          long entryDate = Long.valueOf(nextEntry.getName().substring(String.format("snap_%d_", regionID).length()));
          if (entryDate <= limit.getTime())
            // Skip dates before the limit date
            continue;
          String historyDate = formatter.format(new Date(entryDate));
          String hYear = historyDate.substring(0, 4);
          String hMonth = historyDate.substring(4, 6);
          String hDay = historyDate.substring(6, 8);
          // Sanity check year and alert on bad year format
          if ((Integer.valueOf(hYear) < 2015) || (Integer.valueOf(hYear) > 2016)) {
            System.err.println("Bad entry date parsed from " + nextRegion + " and entry " + nextEntry + ", date parses to " + historyDate + ", skipping");
            continue;
          }
          // Create or append to file for this market history day
          String fileName = String.format("%s_%d_%s.history", prefix, typeID, historyDate);
          File targetFile = new File(new File(new File(new File(outputDir, hYear), hMonth), hDay), fileName);
          targetFile.getParentFile().mkdirs();
          PrintWriter historyFile = new PrintWriter(new FileWriter(targetFile, true));
          try {
            // Copy market snapshot to output file
            InputStreamReader reader = new InputStreamReader(regionZip.getInputStream(nextEntry));
            try {
              char[] buffer = new char[2048];
              int len;
              len = reader.read(buffer);
              while (len != -1) {
                historyFile.write(buffer, 0, len);
                len = reader.read(buffer);
              }
            } finally {
              reader.close();
            }
          } finally {
            historyFile.close();
          }
        }
      } finally {
        regionZip.close();
      }
    }
  }
}
