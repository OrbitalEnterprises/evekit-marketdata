package enterprises.orbital.evekit.marketdata.scheduler;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.base.PersistentProperty;
import enterprises.orbital.db.ConnectionFactory.RunInTransaction;
import enterprises.orbital.db.ConnectionFactory.RunInVoidTransaction;
import enterprises.orbital.db.DBPropertyProvider;
import enterprises.orbital.evekit.marketdata.CRESTClient;
import enterprises.orbital.evekit.marketdata.EveKitMarketDataProvider;
import enterprises.orbital.evekit.marketdata.Instrument;
import enterprises.orbital.evekit.marketdata.MarketHistory;
import enterprises.orbital.evekit.marketdata.Order;
import enterprises.orbital.evekit.marketdata.Region;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.MetricsServlet;

/**
 * Scheduler which doesn't run as a web service and instead downloads all market updates directly.
 */
public class StandaloneScheduler {
  public static final Logger                     log                                = Logger.getLogger(StandaloneScheduler.class.getName());
  // Property which holds the name of the persistence unit for properties
  public static final String                     CREST_ROOT_PROP                    = "enterprises.orbital.evekit.marketdata.crest_root";
  public static final String                     CREST_ROOT_DEFAULT                 = "https://crest.eveonline.com/";
  public static final String                     PROP_APP_PATH                      = "enterprises.orbital.evekit.marketdata-scheduler.apppath";
  public static final String                     DEF_APP_PATH                       = "http://localhost/marketdata-scheduler";
  public static final String                     PROP_INSTRUMENT_UPDATE_INTERVAL    = "enterprises.orbital.evekit.marketdata-scheduler.instUpdateInt";
  public static final long                       DEF_INSTRUMENT_UPDATE_INTERVAL     = TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);
  public static final String                     PROP_STUCK_UPDATE_INTERVAL         = "enterprises.orbital.evekit.marketdata-scheduler.instStuckInt";
  public static final long                       DEF_STUCK_UPDATE_INTERVAL          = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);
  public static final String                     PROP_STUCK_HISTORY_UPDATE_INTERVAL = "enterprises.orbital.evekit.marketdata-scheduler.instStuckHistoryInt";
  public static final long                       DEF_STUCK_HISTORY_UPDATE_INTERVAL  = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
  public static final String                     PROP_STUCK_REGION_UPDATE_INTERVAL  = "enterprises.orbital.evekit.marketdata-scheduler.regionStuckInt";
  public static final long                       DEF_STUCK_REGION_UPDATE_INTERVAL   = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);
  public static final String                     PROP_ORDER_PROC_QUEUE_SIZE         = "enterprises.orbital.evekit.marketdata-scheduler.procQueueSize";
  public static final int                        DEF_ORDER_PROC_QUEUE_SIZE          = 100;
  public static final String                     PROP_BOOK_DIR                      = "enterprises.orbital.evekit.marketdata-scheduler.bookDir";
  public static final String                     DEF_BOOK_DIR                       = "";
  public static final String                     PROP_HISTORY_DIR                   = "enterprises.orbital.evekit.marketdata-scheduler.historyDir";
  public static final String                     DEF_HISTORY_DIR                    = "";
  public static final String                     PROP_REGION_DIR                    = "enterprises.orbital.evekit.marketdata-scheduler.regionDir";
  public static final String                     DEF_REGION_DIR                     = "";
  public static final String                     PROP_MIN_SCHED_INTERVAL            = "enterprises.orbital.evekit.marketdata.scheduler.minSchedInterval";
  public static final long                       DEF_MIN_SCHED_INTERVAL             = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);
  public static final String                     PROP_MIN_HISTORY_INTERVAL          = "enterprises.orbital.evekit.marketdata.scheduler.minHistoryInterval";
  public static final long                       DEF_MIN_HISTORY_INTERVAL           = TimeUnit.MILLISECONDS.convert(20, TimeUnit.HOURS);
  public static final String                     MAX_URL_RETR_POOL_PROP             = "enterprises.orbital.evekit.marketdata.populator.maxURLPool";
  public static final int                        MAX_URL_RETR_POOL_DEF              = 20;
  public static final String                     HISTORY_THREAD_PROP                = "enterprises.orbital.evekit.marketdata.populator.historyThreads";
  public static final int                        HISTORY_THREAD_DEF                 = 1;
  public static final String                     REGION_THREAD_PROP                 = "enterprises.orbital.evekit.marketdata.populator.regionThreads";
  public static final int                        REGION_THREAD_DEF                  = 1;

  // Metrics
  // Histogram: samples are in seconds, bucket size is one minute, max is 4 hours
  public static final Histogram                  all_instrument_update_samples      = Histogram.build().name("all_instrument_update_delay_seconds")
      .help("Interval (seconds) between updates for all instruments.").linearBuckets(0, 60, 240).register();
  // Histogram: samples are in seconds, bucket size is 1 hour, max is 24 hours
  public static final Histogram                  all_history_update_samples         = Histogram.build().name("all_history_update_delay_seconds")
      .help("Interval (seconds) between history updates for all instruments.").linearBuckets(0, 3600, 24).register();
  // Histogram: samples are in seconds, bucket size is one minute, max is 4 hours
  public static final Histogram                  all_region_update_samples          = Histogram.build().name("all_region_update_delay_seconds")
      .help("Interval (seconds) between updates for all regions.").linearBuckets(0, 60, 240).register();
  public static final Histogram                  all_instrument_web_request_samples = Histogram.build().name("all_instrument_web_request_delay_seconds")
      .help("Interval (seconds) between updates for all instruments.").linearBuckets(0, 60, 120).register();
  public static final Histogram                  all_region_download_samples        = Histogram.build().name("all_region_download_delay_seconds")
      .help("Interval (seconds) between updates for all regions.").linearBuckets(0, 10, 120).register();
  public static final Histogram                  all_history_download_samples       = Histogram.build().name("all_history_download_delay_seconds")
      .help("Interval (seconds) between updates for all market history requests.").linearBuckets(0, 10, 120).register();

  protected static Set<Integer>                  regionMap                          = new HashSet<>();
  protected static Set<Integer>                  typeMap                            = new HashSet<>();
  protected static ExecutorService               urlRetrieverPool;
  public static Object[]                         orderProcessingQueues;
  public static Object[]                         historyProcessingQueues;

  protected static final ThreadLocal<DateFormat> dateFormat                         = OrbitalProperties
      .dateFormatFactory(new OrbitalProperties.DateFormatGenerator() {

                                                                                          @Override
                                                                                          public DateFormat generate() {
                                                                                            SimpleDateFormat result = new SimpleDateFormat(
                                                                                                "yyyy-MM-dd'T'HH:mm:ss");
                                                                                            result.setTimeZone(TimeZone.getTimeZone("UTC"));
                                                                                            return result;
                                                                                          }
                                                                                        });

  @SuppressWarnings("unchecked")
  public static void main(
                          String[] argv)
    throws Exception {
    // Populate properties
    OrbitalProperties.addPropertyFile("EveKitMarketdataScheduler.properties");
    // Sent persistence unit for properties
    PersistentProperty.setProvider(new DBPropertyProvider(OrbitalProperties.getGlobalProperty(EveKitMarketDataProvider.MARKETDATA_PU_PROP)));
    // Start metrics servlet
    Server server = new Server(9090);
    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/");
    server.setHandler(context);
    context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
    server.start();
    // Prepare processing queue
    createProcessingQueues();
    // Create thread pool for retrieving remote data
    int poolSize = (int) OrbitalProperties.getLongGlobalProperty(MAX_URL_RETR_POOL_PROP, MAX_URL_RETR_POOL_DEF);
    urlRetrieverPool = Executors.newFixedThreadPool(poolSize);
    // Set agent if present
    String agent = OrbitalProperties.getGlobalProperty("enterprises.orbital.evekit.marketdata.crest.agent", null);
    if (agent != null) CRESTClient.setAgent(agent);
    // Unstick all instruments and regions at startup
    unstickInstruments(0L);
    unstickRegions(0L);
    // Force initial instrument and region refresh
    refreshInstrumentMap();
    refreshRegionMap();
    // Schedule instrument map updater to run on a timer
    (new Thread(
        new MaintenanceRunnable(OrbitalProperties.getLongGlobalProperty(PROP_INSTRUMENT_UPDATE_INTERVAL, DEF_INSTRUMENT_UPDATE_INTERVAL), new Maintenance() {

          @Override
          public boolean performMaintenance() {
            return refreshInstrumentMap();
          }

        }))).start();
    // Schedule region map updater to run on a timer
    (new Thread(
        new MaintenanceRunnable(OrbitalProperties.getLongGlobalProperty(PROP_INSTRUMENT_UPDATE_INTERVAL, DEF_INSTRUMENT_UPDATE_INTERVAL), new Maintenance() {

          @Override
          public boolean performMaintenance() {
            return refreshRegionMap();
          }

        }))).start();
    // Schedule order processing threads
    for (int i = 0; i < orderProcessingQueues.length; i++) {
      (new Thread(new OrderProcessor((ArrayBlockingQueue<List<Order>>) orderProcessingQueues[i]))).start();
    }
    // Schedule history processing threads
    for (int i = 0; i < historyProcessingQueues.length; i++) {
      (new Thread(new HistoryProcessor((ArrayBlockingQueue<List<MarketHistory>>) historyProcessingQueues[i]))).start();
    }
    // Schedule history and region update threads
    int historyThreads = (int) OrbitalProperties.getLongGlobalProperty(HISTORY_THREAD_PROP, HISTORY_THREAD_DEF);
    int regionThreads = (int) OrbitalProperties.getLongGlobalProperty(REGION_THREAD_PROP, REGION_THREAD_DEF);
    for (int i = 0; i < regionThreads; i++) {
      (new Thread(new RegionUpdater())).start();
    }
    for (int i = 0; i < historyThreads; i++) {
      (new Thread(new HistoryUpdater())).start();
    }
    // Schedule stuck instrument cleanup to run on a timer
    (new Thread(new MaintenanceRunnable(OrbitalProperties.getLongGlobalProperty(PROP_STUCK_UPDATE_INTERVAL, DEF_STUCK_UPDATE_INTERVAL), new Maintenance() {

      @Override
      public boolean performMaintenance() {
        long stuckDelay = OrbitalProperties.getLongGlobalProperty(PROP_STUCK_HISTORY_UPDATE_INTERVAL, DEF_STUCK_HISTORY_UPDATE_INTERVAL);
        long threshold = OrbitalProperties.getCurrentTime() - stuckDelay;
        return unstickInstruments(threshold);
      }

    }))).start();
    // Schedule stuck region cleanup to run on a timer
    (new Thread(
        new MaintenanceRunnable(
            OrbitalProperties.getLongGlobalProperty(PROP_STUCK_REGION_UPDATE_INTERVAL, DEF_STUCK_REGION_UPDATE_INTERVAL), new Maintenance() {

              @Override
              public boolean performMaintenance() {
                long stuckDelay = OrbitalProperties.getLongGlobalProperty(PROP_STUCK_REGION_UPDATE_INTERVAL, DEF_STUCK_REGION_UPDATE_INTERVAL);
                long threshold = OrbitalProperties.getCurrentTime() - stuckDelay;
                return unstickRegions(threshold);
              }

            }))).start();
  }

  protected static Set<Integer> getTypeMap() {
    synchronized (Instrument.class) {
      return typeMap;
    }
  }

  protected static Set<Integer> getRegionMap() {
    synchronized (Region.class) {
      return regionMap;
    }
  }

  protected static boolean unstickInstruments(
                                              final long threshold) {
    log.info("Unsticking all instruments");
    synchronized (Instrument.class) {
      try {
        EveKitMarketDataProvider.getFactory().runTransaction(new RunInVoidTransaction() {
          @Override
          public void run() throws Exception {
            for (Instrument delayed : Instrument.getHistoryDelayed(threshold)) {
              // Unschedule delayed instruments
              log.info("Unsticking (history) " + delayed.getTypeID() + " which has been scheduled since " + delayed.getScheduleTime());
              delayed.setHistoryScheduled(false);
              Instrument.update(delayed);
            }
          }
        });
      } catch (Exception e) {
        log.log(Level.SEVERE, "DB error updating instruments, aborting", e);
        return false;
      }
    }
    log.info("Done unsticking instruments");
    return true;
  }

  protected static boolean unstickRegions(
                                          final long threshold) {
    log.info("Unsticking all regions");
    synchronized (Region.class) {
      try {
        EveKitMarketDataProvider.getFactory().runTransaction(new RunInVoidTransaction() {
          @Override
          public void run() throws Exception {
            for (Region delayed : Region.getDelayed(threshold)) {
              // Unschedule delayed regions
              log.info("Unsticking (region) " + delayed.getRegionID() + " which has been scheduled since " + delayed.getScheduleTime());
              delayed.setScheduled(false);
              Region.update(delayed);
            }
          }
        });
      } catch (Exception e) {
        log.log(Level.SEVERE, "DB error updating regions, aborting", e);
        return false;
      }
    }
    log.info("Done unsticking regions");
    return true;
  }

  protected static Set<Integer> populateRegionMap() throws IOException {
    Set<Integer> newRegionSet = new HashSet<>();
    URL root = new URL(OrbitalProperties.getGlobalProperty(CREST_ROOT_PROP, CREST_ROOT_DEFAULT));
    CRESTClient client = new CRESTClient(root);
    String baseHref = client.getData().getJsonObject("regions").getString("href");
    root = new URL(baseHref);
    CompletionService<JsonObject> urlResult = retrieveURLData(Collections.singletonList(root));
    JsonObject data;
    try {
      data = urlResult.take().get();
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
    fillReferenceSet(newRegionSet, data);
    int i, pageCount = data.getInt("pageCount");
    if (pageCount > 1) {
      // Build target list
      List<URL> targets = new ArrayList<>();
      for (i = 2; i <= pageCount; i++) {
        targets.add(new URL(baseHref + "?page=" + i));
      }
      // Queue URL download
      urlResult = retrieveURLData(targets);
      // Process all downloads
      try {
        for (i = 2; i <= pageCount; i++) {
          data = urlResult.take().get();
          fillReferenceSet(newRegionSet, data);
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }
    return newRegionSet;
  }

  protected static Set<Integer> populateTypeMap() throws IOException {
    Set<Integer> newTypeSet = new HashSet<>();
    URL root = new URL(OrbitalProperties.getGlobalProperty(CREST_ROOT_PROP, CREST_ROOT_DEFAULT));
    CRESTClient client = new CRESTClient(root);
    String baseHref = client.getData().getJsonObject("marketTypes").getString("href");
    root = new URL(baseHref);
    CompletionService<JsonObject> urlResult = retrieveURLData(Collections.singletonList(root));
    JsonObject data;
    try {
      data = urlResult.take().get();
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
    fillReferenceSet(newTypeSet, data);
    int i, pageCount = data.getInt("pageCount");
    if (pageCount > 1) {
      // Build target list
      List<URL> targets = new ArrayList<>();
      for (i = 2; i <= pageCount; i++) {
        targets.add(new URL(baseHref + "?page=" + i));
      }
      // Queue URL download
      urlResult = retrieveURLData(targets);
      // Process all downloads
      try {
        for (i = 2; i <= pageCount; i++) {
          data = urlResult.take().get();
          fillReferenceSet(newTypeSet, data);
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }
    return newTypeSet;
  }

  protected static boolean refreshInstrumentMap() {
    log.info("Refreshing instrument map");
    // Retrieve list of all current active instrument IDs
    final Set<Integer> currentActive = new HashSet<Integer>();
    try {
      currentActive.addAll(Instrument.getActiveTypeIDs());
    } catch (Exception e) {
      log.log(Level.SEVERE, "DB error retrieving instruments, aborting refresh", e);
      return false;
    }
    // Retrieve all current instruments from CREST
    final Set<Integer> latestActive = new HashSet<Integer>();
    try {
      latestActive.addAll(populateTypeMap());
    } catch (IOException e) {
      log.log(Level.SEVERE, "Error retrieving last active market types, skipping update", e);
      return false;
    }
    synchronized (Instrument.class) {
      // Add instruments we're missing (active, unscheduled)
      try {
        EveKitMarketDataProvider.getFactory().runTransaction(new RunInVoidTransaction() {
          @Override
          public void run() throws Exception {

            for (int next : latestActive) {
              if (currentActive.contains(next)) continue;
              // Instrument we either haven't seen before, or we already have but have decided to make inactive
              Instrument existing = Instrument.get(next);
              if (existing != null) {
                log.info("Instrument " + next + " already exists but is inactive, leaving inactive");
                continue;
              }
              log.info("Adding new instrument type " + next);
              Instrument newI = new Instrument(next, true, 0L, 0L);
              Instrument.update(newI);
            }
            // De-activate instruments no longer in CREST
            for (int next : currentActive) {
              if (latestActive.contains(next)) continue;
              // Instrument no longer active
              Instrument toDeactivate = Instrument.get(next);
              if (toDeactivate == null) {
                log.severe("Failed to find instrument " + next + " for deactivation, skipping");
              } else {
                log.info("Deactivating missing instrument type " + next);
                toDeactivate.setActive(false);
                Instrument.update(toDeactivate);
              }
            }
          }
        });
      } catch (Exception e) {
        log.log(Level.SEVERE, "DB error updating instruments, aborting", e);
        return false;
      }
      typeMap = latestActive;
    }
    log.info("Instrument map refresh complete");
    return true;
  }

  protected static boolean refreshRegionMap() {
    log.info("Refreshing region map");
    // Retrieve list of all current active region IDs
    final Set<Integer> currentActive = new HashSet<Integer>();
    try {
      currentActive.addAll(Region.getActiveRegionIDs());
    } catch (Exception e) {
      log.log(Level.SEVERE, "DB error retrieving regions, aborting refresh", e);
      return false;
    }
    // Retrieve all current regions from CREST
    final Set<Integer> latestActive = new HashSet<Integer>();
    try {
      latestActive.addAll(populateRegionMap());
    } catch (IOException e) {
      log.log(Level.SEVERE, "Error retrieving last active regions, skipping update", e);
      return false;
    }
    synchronized (Region.class) {
      // Add regions we're missing (active, unscheduled)
      try {
        EveKitMarketDataProvider.getFactory().runTransaction(new RunInVoidTransaction() {
          @Override
          public void run() throws Exception {

            for (int next : latestActive) {
              if (currentActive.contains(next)) continue;
              // Region we either haven't seen before, or we already have but have decided to make inactive
              Region existing = Region.get(next);
              if (existing != null) {
                log.info("Region " + next + " already exists but is inactive, leaving inactive");
                continue;
              }
              log.info("Adding new region " + next);
              Region newI = new Region(next, true, 0L);
              Region.update(newI);
            }
            // De-activate regions no longer in CREST (not sure this can ever happen)
            for (int next : currentActive) {
              if (latestActive.contains(next)) continue;
              // Region no longer active
              Region toDeactivate = Region.get(next);
              if (toDeactivate == null) {
                log.severe("Failed to find region " + next + " for deactivation, skipping");
              } else {
                log.info("Deactivating missing region " + next);
                toDeactivate.setActive(false);
                Region.update(toDeactivate);
              }
            }
          }
        });
      } catch (Exception e) {
        log.log(Level.SEVERE, "DB error updating regions, aborting", e);
        return false;
      }
      regionMap = latestActive;
    }
    log.info("Instrument map refresh complete");
    return true;
  }

  // These functions encapsulate the current queue placement logic for order processors
  protected static void createProcessingQueues() {
    orderProcessingQueues = new Object[10];
    for (int i = 0; i < orderProcessingQueues.length; i++)
      orderProcessingQueues[i] = new ArrayBlockingQueue<List<Order>>(
          (int) OrbitalProperties.getLongGlobalProperty(PROP_ORDER_PROC_QUEUE_SIZE, DEF_ORDER_PROC_QUEUE_SIZE), true);
    historyProcessingQueues = new Object[1];
    for (int i = 0; i < historyProcessingQueues.length; i++)
      historyProcessingQueues[i] = new ArrayBlockingQueue<List<MarketHistory>>(
          (int) OrbitalProperties.getLongGlobalProperty(PROP_ORDER_PROC_QUEUE_SIZE, DEF_ORDER_PROC_QUEUE_SIZE), true);
  }

  // This function encapsulates the queue placement logic for the order processor
  @SuppressWarnings("unchecked")
  public static void queueOrders(
                                 int typeID,
                                 List<Order> orderBlock)
    throws InterruptedException {
    int queue = typeID % orderProcessingQueues.length;
    ((ArrayBlockingQueue<List<Order>>) orderProcessingQueues[queue]).put(orderBlock);
  }

  // This function encapsulates the queue placement logic for the history processor
  @SuppressWarnings("unchecked")
  public static void queueHistory(
                                  int typeID,
                                  List<MarketHistory> historyBlock)
    throws InterruptedException {
    int queue = typeID % historyProcessingQueues.length;
    ((ArrayBlockingQueue<List<MarketHistory>>) historyProcessingQueues[queue]).put(historyBlock);
  }

  protected static List<Order> getOrderList(
                                            Map<Integer, List<Order>> map,
                                            int region) {
    List<Order> list = map.get(region);
    if (list == null) {
      list = new ArrayList<Order>();
      map.put(region, list);
    }
    return list;
  }

  protected static Comparator<Order> bidComparator = new Comparator<Order>() {

                                                     @Override
                                                     public int compare(
                                                                        Order o1,
                                                                        Order o2) {
                                                       // Sort bids highest prices first
                                                       return -o1.getPrice().compareTo(o2.getPrice());
                                                     }

                                                   };

  protected static Comparator<Order> askComparator = new Comparator<Order>() {

                                                     @Override
                                                     public int compare(
                                                                        Order o1,
                                                                        Order o2) {
                                                       // Sort asks lowest prices first
                                                       return o1.getPrice().compareTo(o2.getPrice());
                                                     }

                                                   };

  protected static void writeOrder(
                                   Order o,
                                   PrintWriter out) {
    out.format("%d,%d,%d,%b,%d,%.2f,%d,%d,%d,%s,%d,%d\n", o.getRegionID(), o.getTypeID(), o.getOrderID(), o.isBuy(), o.getIssued(), o.getPrice(),
               o.getVolumeEntered(), o.getMinVolume(), o.getVolume(), o.getOrderRange(), o.getLocationID(), o.getDuration());
  }

  protected static void writeHistory(
                                     MarketHistory h,
                                     PrintWriter out) {
    out.format("%d,%d,%d,%.2f,%.2f,%.2f,%d,%d\n", h.getTypeID(), h.getRegionID(), h.getOrderCount(), h.getLowPrice(), h.getHighPrice(), h.getAvgPrice(),
               h.getVolume(), h.getDate());
  }

  protected static MarketHistory readHistory(
                                             Path src)
    throws IOException {
    // Convert from write format above
    List<String> data = Files.readAllLines(src, StandardCharsets.UTF_8);
    String[] parse = data.get(0).split(",");
    int typeID = Integer.valueOf(parse[0]);
    int regionID = Integer.valueOf(parse[1]);
    int orderCount = Integer.valueOf(parse[2]);
    BigDecimal lowPrice = BigDecimal.valueOf(Double.valueOf(parse[3]).doubleValue()).setScale(2, RoundingMode.HALF_UP);
    BigDecimal highPrice = BigDecimal.valueOf(Double.valueOf(parse[4]).doubleValue()).setScale(2, RoundingMode.HALF_UP);
    BigDecimal avgPrice = BigDecimal.valueOf(Double.valueOf(parse[5]).doubleValue()).setScale(2, RoundingMode.HALF_UP);
    long volume = Long.valueOf(parse[6]);
    long date = Long.valueOf(parse[7]);
    return new MarketHistory(typeID, regionID, orderCount, lowPrice, highPrice, avgPrice, volume, date);
  }

  protected static void writeBookSnap(
                                      long at,
                                      int typeID,
                                      int regionID,
                                      List<Order> bids,
                                      List<Order> asks)
    throws IOException {
    Map<String, String> env = new HashMap<>();
    env.put("create", "true");
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    String bookFileName = String.format("book_%d_%s.zip", regionID, formatter.format(new Date(at)));
    String snapEntryName = String.format("snap_%d", at);
    Path dir = Paths.get(OrbitalProperties.getGlobalProperty(PROP_BOOK_DIR, DEF_BOOK_DIR), "books", String.valueOf(typeID));
    Files.createDirectories(dir);
    Path file = Paths.get(OrbitalProperties.getGlobalProperty(PROP_BOOK_DIR, DEF_BOOK_DIR), "books", String.valueOf(typeID), bookFileName);
    URI outURI = URI.create("jar:file:" + file.toUri().toString().substring("file://".length()));
    try (FileSystem fs = FileSystems.newFileSystem(outURI, env)) {
      Path entry = fs.getPath(snapEntryName);
      try (PrintWriter snapOut = new PrintWriter(Files.newBufferedWriter(entry, StandardCharsets.UTF_8, StandardOpenOption.CREATE))) {
        // Write header
        snapOut.format("%d\n%d\n", bids.size(), asks.size());
        // Dump book
        for (Order next : bids)
          writeOrder(next, snapOut);
        for (Order next : asks)
          writeOrder(next, snapOut);
      }
    }
  }

  protected static void writeRegionSnap(
                                        long at,
                                        int regionID,
                                        List<Order> orders)
    throws IOException {
    Map<String, String> env = new HashMap<>();
    env.put("create", "true");
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    String regionFileName = String.format("region_%d_%s.gz", at, formatter.format(new Date(at)));
    Path dir = Paths.get(OrbitalProperties.getGlobalProperty(PROP_REGION_DIR, DEF_REGION_DIR), "regions", String.valueOf(regionID));
    Files.createDirectories(dir);
    Path file = Paths.get(OrbitalProperties.getGlobalProperty(PROP_REGION_DIR, DEF_REGION_DIR), "regions", String.valueOf(regionID), regionFileName);
    try (PrintWriter snapOut = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file.toFile()))))) {
      // Write header
      snapOut.format("%d\n", orders.size());
      // Dump all orders
      for (Order next : orders) {
        writeOrder(next, snapOut);
      }
    }
  }

  protected static void writeBulkHistorySnap(
                                             long at,
                                             int typeID,
                                             int regionID,
                                             List<MarketHistory> history)
    throws IOException {
    // History files are stored in a zip with records listed by date.
    // The history file is organized by typeID and the date the snapshot was recorded:
    //
    // <root>/history/<typeID>/history_<regionID>_<snapDateString>.zip
    //
    // Within this file, records are indexed by the regionID and date listed in the MarketHistory object:
    //
    // snap_<regionid>_<markethistory_date_millis>
    //
    // Records are only updated if they have changed.
    Map<String, String> env = new HashMap<>();
    env.put("create", "true");
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    String historyFileName = String.format("history_%d_%s.zip", regionID, formatter.format(new Date(at)));
    Path dir = Paths.get(OrbitalProperties.getGlobalProperty(PROP_HISTORY_DIR, DEF_HISTORY_DIR), "history", String.valueOf(typeID));
    Files.createDirectories(dir);
    Path file = Paths.get(OrbitalProperties.getGlobalProperty(PROP_HISTORY_DIR, DEF_HISTORY_DIR), "history", String.valueOf(typeID), historyFileName);
    URI outURI = URI.create("jar:file:" + file.toUri().toString().substring("file://".length()));
    long dateLowerBound = OrbitalProperties.getCurrentTime() - TimeUnit.MILLISECONDS.convert(700, TimeUnit.DAYS);
    long dateUpperBound = OrbitalProperties.getCurrentTime();
    try (FileSystem fs = FileSystems.newFileSystem(outURI, env)) {
      for (MarketHistory nextHistory : history) {
        // Filter bad entry dates
        if (nextHistory.getDate() < dateLowerBound || nextHistory.getDate() > dateUpperBound) {
          log.warning("Discarding history with obviously bad date: " + nextHistory.getDate());
          continue;
        }
        // Store data
        String snapEntryName = String.format("snap_%d_%d", regionID, nextHistory.getDate());
        Path entry = fs.getPath(snapEntryName);
        // If file exists we can exit as snaps are immutable
        if (Files.exists(entry)) return;
        try (PrintWriter snapOut = new PrintWriter(Files.newBufferedWriter(entry, StandardCharsets.UTF_8, StandardOpenOption.CREATE))) {
          // Each snap contains a single MarketHistory record
          writeHistory(nextHistory, snapOut);
        }
      }
    }
  }

  public static List<MarketHistory> populateHistory(
                                                    final int region,
                                                    final int type)
    throws IOException {
    // Setup for history retrieval
    // Href currently NOT discoverable from CREST root so we hand construct, e.g. https://crest-tq.eveonline.com/market/10000002/types/34/history/
    String historyHref = OrbitalProperties.getGlobalProperty(CREST_ROOT_PROP, CREST_ROOT_DEFAULT) + "market/" + region + "/types/" + type + "/history/";
    // Retrieve and populate first page of history
    URL root = new URL(historyHref);
    List<MarketHistory> toStore = new ArrayList<MarketHistory>();
    CompletionService<JsonObject> urlResult = retrieveURLData(Collections.singletonList(root));
    JsonObject data;
    try {
      data = urlResult.take().get();
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
    popMarketHistoryFromResult(region, type, data, toStore);
    // Populate data and queue up any remaining pages for retrieval
    int i, pageCount = data.getInt("pageCount");
    if (pageCount > 1) {
      // Build target list
      List<URL> targets = new ArrayList<>();
      for (i = 2; i <= pageCount; i++) {
        targets.add(new URL(historyHref + "?page=" + i));
      }
      // Queue URL download
      urlResult = retrieveURLData(targets);
      // Process all downloads
      try {
        for (i = 2; i <= pageCount; i++) {
          data = urlResult.take().get();
          popMarketHistoryFromResult(region, type, data, toStore);
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }
    // Return result
    return toStore;
  }

  protected static CompletionService<JsonObject> retrieveURLData(
                                                                 List<URL> targets) {
    CompletionService<JsonObject> ecs = new ExecutorCompletionService<JsonObject>(urlRetrieverPool);
    for (final URL next : targets) {
      ecs.submit(new Callable<JsonObject>() {

        @Override
        public JsonObject call() throws Exception {
          CRESTClient client = new CRESTClient(next);
          return client.getData();
        }

      });

    }
    return ecs;
  }

  protected static void popMarketHistoryFromResult(
                                                   int region,
                                                   int type,
                                                   JsonObject data,
                                                   List<MarketHistory> storage)
    throws IOException {
    JsonArray batch = data.getJsonArray("items");
    for (JsonObject next : batch.getValuesAs(JsonObject.class)) {
      try {
        storage.add(convertMarketHistory(region, type, next));
      } catch (Exception e) {
        log.log(Level.WARNING, "Parsing failed on object: " + next, e);
        throw new IOException(e);
      }
    }
  }

  protected static void popRegionOrdersFromResult(
                                                  int region,
                                                  Set<Integer> typeFilter,
                                                  JsonObject data,
                                                  List<Order> storage)
    throws IOException {
    JsonArray batch = data.getJsonArray("items");
    for (JsonObject next : batch.getValuesAs(JsonObject.class)) {
      try {
        Order nextOrder = convertRegionOrder(region, next);
        // Only add known types. Apparently the market in some regions contains types not in the market types list.
        if (typeFilter.contains(nextOrder.getTypeID())) storage.add(nextOrder);
      } catch (Exception e) {
        log.log(Level.WARNING, "Parsing failed on object: " + next, e);
        throw new IOException(e);
      }
    }
  }

  protected static void storeRegionResults(
                                           int region,
                                           List<Order> toStore)
    throws IOException {
    long at = OrbitalProperties.getCurrentTime();
    writeRegionSnap(at, region, toStore);
    // Queue up orders for processing. We'll block if the queue is backlogged.
    // try {
    // // Organize orders by type and queue
    // Map<Integer, List<Order>> orderMap = new HashMap<>();
    // for (Order next : toStore) {
    // List<Order> orderList = orderMap.get(next.getTypeID());
    // if (orderList == null) {
    // orderList = new ArrayList<>();
    // orderMap.put(next.getTypeID(), orderList);
    // }
    // orderList.add(next);
    // }
    // for (int type : orderMap.keySet()) {
    // List<Order> orderList = orderMap.get(type);
    // queueOrders(type, orderList);
    // }
    // } catch (InterruptedException e) {
    // log.log(Level.INFO, "Break requested, exiting", e);
    // System.exit(0);
    // }
  }

  public static void populateRegion(
                                    final int region,
                                    final Set<Integer> typeMap)
    throws IOException {
    // Setup for region retrieval
    String regionHref = OrbitalProperties.getGlobalProperty(CREST_ROOT_PROP, CREST_ROOT_DEFAULT) + "market/" + region + "/orders/all/";
    // Retrieve and populate orders
    long startPopTime = OrbitalProperties.getCurrentTime();
    List<Order> toStore = new ArrayList<Order>();
    // Retrieve first page and process
    URL root = new URL(regionHref);
    CompletionService<JsonObject> urlResult = retrieveURLData(Collections.singletonList(root));
    JsonObject data;
    try {
      data = urlResult.take().get();
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
    // Populate data and queue up any remaining pages for retrieval
    popRegionOrdersFromResult(region, typeMap, data, toStore);
    log.info("Accumulated " + toStore.size() + " results for region " + region);
    int i, pageCount = data.getInt("pageCount");
    if (pageCount > 1) {
      // Build target list
      List<URL> targets = new ArrayList<>();
      for (i = 2; i <= pageCount; i++) {
        targets.add(new URL(regionHref + "?page=" + i));
      }
      // Queue URL download
      urlResult = retrieveURLData(targets);
      // Process all downloads
      try {
        for (i = 2; i <= pageCount; i++) {
          data = urlResult.take().get();
          // log.info("Preparing results for region " + region);
          popRegionOrdersFromResult(region, typeMap, data, toStore);
          log.info("Accumulated " + toStore.size() + " results for region " + region);
          // log.info("Sending " + toStore.size() + " results for region " + region);
          // storeRegionResults(region, toStore, schedulerAPI);
          // log.info("Stored " + toStore.size() + " results for region " + region);
          // toStore.clear();
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }
    log.info("Sending " + toStore.size() + " results for region " + region);
    storeRegionResults(region, toStore);
    log.info("Stored " + toStore.size() + " results for region " + region);
    long endDownloadTime = OrbitalProperties.getCurrentTime();
    all_region_download_samples.observe((endDownloadTime - startPopTime) / 1000);
  }

  protected static Order convertRegionOrder(
                                            int regionID,
                                            JsonObject order)
    throws ParseException {
    // Extract fields
    int typeID = order.getJsonNumber("type").intValue();
    long orderID = order.getJsonNumber("id").longValue();
    boolean buy = order.getBoolean("buy");
    long issued = dateFormat.get().parse(order.getString("issued")).getTime();
    BigDecimal price = BigDecimal.valueOf(order.getJsonNumber("price").doubleValue()).setScale(2, RoundingMode.HALF_UP);
    int volumeEntered = order.getInt("volumeEntered");
    int minVolume = order.getInt("minVolume");
    int volume = order.getInt("volume");
    String range = order.getString("range");
    long locationID = order.getJsonNumber("stationID").longValue();
    int duration = order.getInt("duration");
    Order result = new Order(regionID, typeID, orderID, buy, issued, price, volumeEntered, minVolume, volume, range, locationID, duration);
    return result;
  }

  protected static MarketHistory convertMarketHistory(
                                                      int regionID,
                                                      int typeID,
                                                      JsonObject history)
    throws ParseException {
    // Extract fields
    int orderCount = history.getJsonNumber("orderCount").intValue();
    BigDecimal lowPrice = BigDecimal.valueOf(history.getJsonNumber("lowPrice").doubleValue()).setScale(2, RoundingMode.HALF_UP);
    BigDecimal highPrice = BigDecimal.valueOf(history.getJsonNumber("highPrice").doubleValue()).setScale(2, RoundingMode.HALF_UP);
    BigDecimal avgPrice = BigDecimal.valueOf(history.getJsonNumber("avgPrice").doubleValue()).setScale(2, RoundingMode.HALF_UP);
    long volume = history.getJsonNumber("volume").longValue();
    long date = dateFormat.get().parse(history.getString("date")).getTime();
    long dateLowerBound = OrbitalProperties.getCurrentTime() - TimeUnit.MILLISECONDS.convert(700, TimeUnit.DAYS);
    long dateUpperBound = OrbitalProperties.getCurrentTime();
    if (date < dateLowerBound || date > dateUpperBound) { throw new ParseException("History has obviously bad date: " + date, 0); }
    MarketHistory result = new MarketHistory(typeID, regionID, orderCount, lowPrice, highPrice, avgPrice, volume, date);
    return result;
  }

  protected static class OrderProcessor implements Runnable {
    private ArrayBlockingQueue<List<Order>> source;

    public OrderProcessor(ArrayBlockingQueue<List<Order>> source) {
      this.source = source;
    }

    @Override
    public void run() {

      while (true) {
        try {
          // Retrieve next order batch from queue. Short circuit on empty batches (should never happen)
          List<Order> nextBatch = source.take();
          assert !nextBatch.isEmpty();
          final int typeID = nextBatch.get(0).getTypeID();
          // Update time depends on when this item makes it to the front of the queue
          final long at = OrbitalProperties.getCurrentTime();
          // Group orders by region and sort
          // region -> bids, region -> asks
          Map<Integer, List<Order>> regionBids = new HashMap<Integer, List<Order>>();
          Map<Integer, List<Order>> regionAsks = new HashMap<Integer, List<Order>>();
          Set<Integer> regionSet = new HashSet<Integer>();
          for (Order next : nextBatch) {
            int regionID = next.getRegionID();
            regionSet.add(regionID);
            getOrderList(next.isBuy() ? regionBids : regionAsks, regionID).add(next);
          }
          // Sort all order lists
          for (List<Order> nextBids : regionBids.values()) {
            Collections.sort(nextBids, bidComparator);
          }
          for (List<Order> nextAsks : regionAsks.values()) {
            Collections.sort(nextAsks, askComparator);
          }
          // Dump each book to the appropriate file
          for (int regionID : regionSet) {
            List<Order> bids = regionBids.get(regionID);
            List<Order> asks = regionAsks.get(regionID);
            writeBookSnap(at, typeID, regionID, bids != null ? bids : Collections.<Order> emptyList(), asks != null ? asks : Collections.<Order> emptyList());
          }
          // try {
          // // Release instrument since we've finished
          // long updateDelay = 0;
          // synchronized (Instrument.class) {
          // updateDelay = EveKitMarketDataProvider.getFactory().runTransaction(new RunInTransaction<Long>() {
          // @Override
          // public Long run() throws Exception {
          // // Update complete - release this instrument
          // Instrument update = Instrument.get(typeID);
          // if (update == null) {
          // log.warning("Unknown instrument " + typeID + ", skipping");
          // return 0L;
          // }
          // long last = update.getLastUpdate();
          // update.setLastUpdate(at);
          // update.setScheduled(false);
          // Instrument.update(update);
          // return at - last;
          // }
          // });
          // }
          // // Store update delay metrics
          // all_instrument_update_samples.observe(updateDelay / 1000);
          // } catch (Exception e) {
          // log.log(Level.SEVERE, "DB error storing order, failing: (" + typeID + ")", e);
          // }
        } catch (InterruptedException e) {
          log.log(Level.INFO, "Break requested, exiting", e);
          System.exit(0);
        } catch (Exception f) {
          log.log(Level.SEVERE, "Fatal error caught in order processing loop, logging and attempting to continue", f);
        }
      }
    }
  }

  protected static class HistoryProcessor implements Runnable {
    private ArrayBlockingQueue<List<MarketHistory>> source;

    public HistoryProcessor(ArrayBlockingQueue<List<MarketHistory>> source) {
      this.source = source;
    }

    @Override
    public void run() {

      while (true) {
        try {
          // Retrieve next history batch from queue. Short circuit on empty batches (should never happen)
          List<MarketHistory> nextBatch = source.take();
          assert !nextBatch.isEmpty();
          final int typeID = nextBatch.get(0).getTypeID();
          // Dump each history item to the appropriate file unless the file already exists and is unchanged.
          final long at = OrbitalProperties.getCurrentTime();
          Map<Integer, List<MarketHistory>> regionMap = new HashMap<Integer, List<MarketHistory>>();
          for (MarketHistory next : nextBatch) {
            int regionID = next.getRegionID();
            List<MarketHistory> group = regionMap.get(regionID);
            if (group == null) {
              group = new ArrayList<MarketHistory>();
              regionMap.put(regionID, group);
            }
            group.add(next);
          }
          for (int regionID : regionMap.keySet()) {
            writeBulkHistorySnap(at, typeID, regionID, regionMap.get(regionID));
          }
          try {
            // Release instrument since we've finished
            long updateDelay = 0;
            synchronized (Instrument.class) {
              updateDelay = EveKitMarketDataProvider.getFactory().runTransaction(new RunInTransaction<Long>() {
                @Override
                public Long run() throws Exception {
                  // Update complete - release this instrument
                  Instrument update = Instrument.get(typeID);
                  long last = update.getLastHistoryUpdate();
                  update.setLastHistoryUpdate(at);
                  update.setHistoryScheduled(false);
                  Instrument.update(update);
                  return at - last;
                }
              });
            }
            // Store update delay metrics
            all_history_update_samples.observe(updateDelay / 1000);
          } catch (Exception e) {
            log.log(Level.SEVERE, "DB error storing history, failing: (" + typeID + ")", e);
          }
        } catch (InterruptedException e) {
          log.log(Level.INFO, "Break requested, exiting", e);
          System.exit(0);
        } catch (Exception f) {
          log.log(Level.SEVERE, "Fatal error caught in history processing loop, logging and attempting to continue", f);
        }
      }
    }
  }

  protected static class RegionUpdater implements Runnable {
    private static final Logger log = Logger.getLogger(RegionUpdater.class.getName());

    @Override
    public void run() {
      while (true) {
        try {
          log.info("Starting region updater thread");
          long interval = PersistentProperty.getLongPropertyWithFallback(PROP_MIN_SCHED_INTERVAL, DEF_MIN_SCHED_INTERVAL);
          // Spin forever updating regions
          while (true) {
            // Attempt to retrieve region to update, otherwise loop and sleep
            Region toPop = null;
            synchronized (Region.class) {
              toPop = Region.takeNextScheduled(interval);
            }
            if (toPop == null) {
              try {
                log.info("No region available for update, waiting");
                Thread.sleep(TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS));
              } catch (InterruptedException f) {
                log.log(Level.WARNING, "Interrupted, exiting", f);
                System.exit(0);
              }
              continue;
            }
            // Capture region, order and type maps for this run
            log.fine("Starting population on: " + toPop);
            long startPopTime = OrbitalProperties.getCurrentTime();
            final int regionPop = toPop.getRegionID();
            // Populate region with this ID
            try {
              Set<Integer> typeMap = getTypeMap();
              populateRegion(regionPop, typeMap);
              log.fine("Populated (" + regionPop + ")");
            } catch (IOException e) {
              log.log(Level.SEVERE, "Failed to populate (" + regionPop + ")", e);
            }
            // Release region
            final long at = OrbitalProperties.getCurrentTime();
            synchronized (Region.class) {
              try {
                long updateDelay = EveKitMarketDataProvider.getFactory().runTransaction(new RunInTransaction<Long>() {
                  @Override
                  public Long run() throws Exception {
                    // Update complete - release this region
                    Region update = Region.get(regionPop);
                    long last = update.getLastUpdate();
                    update.setLastUpdate(at);
                    update.setScheduled(false);
                    Region.update(update);
                    return at - last;
                  }
                });
                all_region_update_samples.observe(updateDelay / 1000);
              } catch (Exception e) {
                log.log(Level.SEVERE, "DB error releasing region, failing: (" + regionPop + ")", e);
              }
            }

            long endPopTime = OrbitalProperties.getCurrentTime();
            log.info("Completed population of region " + regionPop + " in time: " + TimeUnit.SECONDS.convert(endPopTime - startPopTime, TimeUnit.MILLISECONDS)
                + " s");
          }
        } catch (Exception e) {
          log.log(Level.SEVERE, "Unexpected error in populator thread, attempting to restart", e);
        }
      }
    }
  }

  protected static class HistoryUpdater implements Runnable {
    private static final Logger log = Logger.getLogger(HistoryUpdater.class.getName());

    @Override
    public void run() {
      while (true) {
        try {
          log.info("Starting history updater thread");
          // Spin forever updating market history
          long interval = PersistentProperty.getLongPropertyWithFallback(PROP_MIN_HISTORY_INTERVAL, DEF_MIN_HISTORY_INTERVAL);
          while (true) {
            // Attempt to retrieve instrument to update, otherwise loop and sleep
            Instrument next;
            synchronized (Instrument.class) {
              next = Instrument.takeNextHistoryScheduled(interval);
            }
            if (next == null) {
              try {
                log.info("No instrument available for update, waiting");
                Thread.sleep(TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS));
              } catch (InterruptedException f) {
                log.log(Level.WARNING, "Interrupted, exiting", f);
                System.exit(0);
              }
              continue;
            }
            // Capture region, order and type maps for this run
            log.fine("Starting history population on: " + next);
            final long startPopTime = OrbitalProperties.getCurrentTime();
            Set<Integer> regionMap = getRegionMap();
            final int typePop = next.getTypeID();
            // Collect all history updates
            List<MarketHistory> toStore = new ArrayList<MarketHistory>();
            // Populate each region for this type
            for (int regionID : regionMap) {
              try {
                toStore.addAll(populateHistory(regionID, typePop));
                log.fine("Populated (" + regionID + ", " + typePop + ")");
              } catch (IOException e) {
                log.log(Level.SEVERE, "Failed to populate (" + regionID + ", " + typePop + ")", e);
              }
            }
            long endDownloadTime = OrbitalProperties.getCurrentTime();
            all_history_download_samples.observe((endDownloadTime - startPopTime) / 1000);
            if (!toStore.isEmpty()) {
              // Queue up history for storage
              queueHistory(typePop, toStore);
            } else {
              // Release instrument since we didn't have anything to queue
              synchronized (Instrument.class) {
                try {
                  EveKitMarketDataProvider.getFactory().runTransaction(new RunInVoidTransaction() {
                    @Override
                    public void run() throws Exception {
                      // Update complete - release this instrument
                      Instrument update = Instrument.get(typePop);
                      update.setLastHistoryUpdate(startPopTime);
                      update.setHistoryScheduled(false);
                      Instrument.update(update);
                    }
                  });
                  long updateDelay = OrbitalProperties.getCurrentTime() - startPopTime;
                  all_history_update_samples.observe(updateDelay / 1000);
                } catch (Exception e) {
                  log.log(Level.SEVERE, "DB error storing order, failing: (" + typePop + ")", e);
                }
              }
            }

            long endPopTime = OrbitalProperties.getCurrentTime();
            log.info("Completed population of type " + typePop + " in time: " + TimeUnit.SECONDS.convert(endPopTime - startPopTime, TimeUnit.MILLISECONDS)
                + " s");
          }
        } catch (Exception e) {
          log.log(Level.SEVERE, "Unexpected error in populator thread, attempting to restart", e);
        }
      }
    }
  }

  protected static void fillReferenceSet(
                                         Set<Integer> regions,
                                         JsonObject data) {
    JsonArray batch = data.getJsonArray("items");
    for (JsonObject next : batch.getValuesAs(JsonObject.class)) {
      regions.add(next.getInt("id"));
    }
  }

  protected static interface Maintenance {
    public boolean performMaintenance();
  }

  protected static class MaintenanceRunnable implements Runnable {
    long        lastRun = 0L;
    long        updateInterval;
    Maintenance action;

    public MaintenanceRunnable(long interval, Maintenance task) {
      updateInterval = interval;
      action = task;
    }

    @Override
    public void run() {
      while (true) {
        long now = OrbitalProperties.getCurrentTime();
        if (now - lastRun > updateInterval) {
          if (action.performMaintenance()) lastRun = OrbitalProperties.getCurrentTime();
        } else {
          try {
            Thread.sleep(updateInterval - (now - lastRun));
          } catch (InterruptedException e) {
            // Servlet shutting down, exit
            System.err.println("Servlet shutdown, exiting run loop");
            return;
          }
        }
      }
    }

  }

}
