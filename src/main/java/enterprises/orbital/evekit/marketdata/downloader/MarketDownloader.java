package enterprises.orbital.evekit.marketdata.downloader;

import java.io.IOException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.base.PersistentProperty;
import enterprises.orbital.db.ConnectionFactory.RunInVoidTransaction;
import enterprises.orbital.db.DBPropertyProvider;
import enterprises.orbital.evekit.marketdata.model.EveKitMarketDataProvider;
import enterprises.orbital.evekit.marketdata.model.Instrument;
import enterprises.orbital.evekit.marketdata.model.Region;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.MetricsServlet;

/**
 * Scheduler which doesn't run as a web service and instead downloads all market updates directly.
 */
public class MarketDownloader {
  public static final Logger                     log                                   = Logger.getLogger(MarketDownloader.class.getName());
  // CREST root
  public static final String                     PROP_CREST_ROOT                       = "enterprises.orbital.evekit.marketdata.crest.root";
  public static final String                     DFLT_CREST_ROOT                       = "https://crest.eveonline.com/";
  // CREST agent
  public static final String                     PROP_CREST_AGENT                      = "enterprises.orbital.evekit.marketdata.crest.agent";
  public static final String                     DFLT_CREST_AGENT                      = null;
  // CREST connect timeout
  public static final String                     PROP_CREST_CONNECT_TIMEOUT            = "enterprises.orbital.evekit.marketdata.crest.connectTimeout";
  public static final int                        DFLT_CREST_CONNECT_TIMEOUT            = 300000;
  // Interval between instrument and region updates against CREST reference data
  public static final String                     PROP_REF_UPDATE_INTERVAL              = "enterprises.orbital.evekit.marketdata.refUpdateInt";
  public static final long                       DFLT_REF_UPDATE_INTERVAL              = TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);
  // How frequently we check for stuck market history downloads
  public static final String                     PROP_STUCK_INSTRUMENT_UPDATE_INTERVAL = "enterprises.orbital.evekit.marketdata.instStuckInt";
  public static final long                       DFLT_STUCK_INSTRUMENT_UPDATE_INTERVAL = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);
  // How frequently we check for stuck region data downloads
  public static final String                     PROP_STUCK_REGION_UPDATE_INTERVAL     = "enterprises.orbital.evekit.marketdata.regionStuckInt";
  public static final long                       DFLT_STUCK_REGION_UPDATE_INTERVAL     = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);
  // Directory where raw history files will be stored
  public static final String                     PROP_HISTORY_DIR                      = "enterprises.orbital.evekit.marketdata.historyDir";
  public static final String                     DFLT_HISTORY_DIR                      = "";
  // Directory where raw price files will be stored
  public static final String                     PROP_PRICE_DIR                        = "enterprises.orbital.evekit.marketdata.priceDir";
  public static final String                     DFLT_PRICE_DIR                        = "";
  // Directory where raw region files will be stored
  public static final String                     PROP_REGION_DIR                       = "enterprises.orbital.evekit.marketdata.regionDir";
  public static final String                     DFLT_REGION_DIR                       = "";
  // Minimum delay between region data refreshes
  public static final String                     PROP_MIN_REGION_SCHED_INTERVAL        = "enterprises.orbital.evekit.marketdata.minRegionSchedInterval";
  public static final long                       DFLT_MIN_REGION_SCHED_INTERVAL        = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);
  // Minimum delay between history data refreshes
  public static final String                     PROP_MIN_HISTORY_SCHED_INTERVAL       = "enterprises.orbital.evekit.marketdata.minHistorySchedInterval";
  public static final long                       DFLT_MIN_HISTORY_SCHED_INTERVAL       = TimeUnit.MILLISECONDS.convert(20, TimeUnit.HOURS);
  // Minimum delay between price data refreshes
  public static final String                     PROP_MIN_PRICE_SCHED_INTERVAL         = "enterprises.orbital.evekit.marketdata.minPriceSchedInterval";
  public static final long                       DFLT_MIN_PRICE_SCHED_INTERVAL         = TimeUnit.MILLISECONDS.convert(20, TimeUnit.HOURS);
  // Max number of threads to use for CREST http requests
  public static final String                     PROP_MAX_URL_RETR_POOL                = "enterprises.orbital.evekit.marketdata.maxURLPool";
  public static final int                        DFLT_MAX_URL_RETR_POOL                = 20;
  // Number of threads to use for market history downloads
  public static final String                     PROP_HISTORY_THREAD                   = "enterprises.orbital.evekit.marketdata.historyThreads";
  public static final int                        DFLT_HISTORY_THREAD                   = 1;
  // Number of threads to use for region data downloads
  public static final String                     PROP_REGION_THREAD                    = "enterprises.orbital.evekit.marketdata.regionThreads";
  public static final int                        DFLT_REGION_THREAD                    = 1;
  // Metrics
  // Histogram: samples are in seconds, bucket size is 1 hour, max is 24 hours
  public static final Histogram                  all_history_update_samples            = Histogram.build().name("all_history_update_delay_seconds")
      .help("Interval (seconds) between history updates for all instruments.").linearBuckets(0, 3600, 24).register();
  // Histogram: samples are in seconds, bucket size is 1 hour, max is 24 hours
  public static final Histogram                  all_price_update_samples              = Histogram.build().name("all_price_update_delay_seconds")
      .help("Interval (seconds) between price updates for all instruments.").linearBuckets(0, 3600, 24).register();
  // Histogram: samples are in seconds, bucket size is one minute, max is 4 hours
  public static final Histogram                  all_region_update_samples             = Histogram.build().name("all_region_update_delay_seconds")
      .help("Interval (seconds) between updates for all regions.").linearBuckets(0, 60, 240).register();
  // Histogram: samples are in seconds, bucket size is 10 seconds, max is 2 minutes
  public static final Histogram                  all_region_download_samples           = Histogram.build().name("all_region_download_delay_seconds")
      .help("Interval (seconds) between updates for all regions.").linearBuckets(0, 10, 120).register();
  // Histogram: samples are in seconds, bucket size is 10 seconds, max is 2 minutes
  public static final Histogram                  all_history_download_samples          = Histogram.build().name("all_history_download_delay_seconds")
      .help("Interval (seconds) between updates for all market history requests.").linearBuckets(0, 10, 120).register();
  // Histogram: samples are in seconds, bucket size is 10 seconds, max is 2 minutes
  public static final Histogram                  all_price_download_samples            = Histogram.build().name("all_price_download_delay_seconds")
      .help("Interval (seconds) between updates for all market price requests.").linearBuckets(0, 10, 120).register();
  // Cache of regions to download
  protected static Set<Integer>                  regionMap                             = new HashSet<>();
  // Cache of types for which history will be fetched
  protected static Set<Integer>                  typeMap                               = new HashSet<>();
  // Thread pool for CREST requests
  protected static ExecutorService               urlRetrieverPool;
  // Thread safe date formatter
  protected static final ThreadLocal<DateFormat> dateFormat                            = OrbitalProperties
      .dateFormatFactory(new OrbitalProperties.DateFormatGenerator() {

                                                                                             @Override
                                                                                             public DateFormat generate() {
                                                                                               SimpleDateFormat result = new SimpleDateFormat(
                                                                                                   "yyyy-MM-dd'T'HH:mm:ss");
                                                                                               result.setTimeZone(TimeZone.getTimeZone("UTC"));
                                                                                               return result;
                                                                                             }
                                                                                           });

  /**
   * The downloader is designed to run as a single process. See README.md for instructions on how to configure. No command line arguments.
   * 
   * @param argv
   *          ignored
   */
  public static void main(
                          String[] argv)
    throws Exception {
    // Populate properties
    OrbitalProperties.addPropertyFile("EveKitMarketdata.properties");
    // Sent persistence unit for properties
    PersistentProperty.setProvider(new DBPropertyProvider(OrbitalProperties.getGlobalProperty(EveKitMarketDataProvider.PROP_MARKETDATA_PU)));
    // Start metrics servlet for Prometheus
    Server server = new Server(9090);
    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/");
    server.setHandler(context);
    context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
    server.start();
    // Create CREST request thread pool
    int poolSize = (int) OrbitalProperties.getLongGlobalProperty(PROP_MAX_URL_RETR_POOL, DFLT_MAX_URL_RETR_POOL);
    urlRetrieverPool = Executors.newFixedThreadPool(poolSize);
    // Set CREST settings
    String agent = OrbitalProperties.getGlobalProperty(PROP_CREST_AGENT, DFLT_CREST_AGENT);
    if (agent != null) CRESTClient.setAgent(agent);
    int connectTimeout = (int) OrbitalProperties.getLongGlobalProperty(PROP_CREST_CONNECT_TIMEOUT, DFLT_CREST_CONNECT_TIMEOUT);
    if (connectTimeout > 0) CRESTClient.setConnectTimeout(connectTimeout);
    // Force initial instrument and region refresh
    refreshInstrumentMap();
    refreshRegionMap();
    // Unstick all instruments and regions at startup
    unstickInstruments(0L);
    unstickRegions(0L);
    // Start maintenance threads
    startMaintenanceThreads();
    // Schedule history and region update threads
    int historyThreads = (int) OrbitalProperties.getLongGlobalProperty(PROP_HISTORY_THREAD, DFLT_HISTORY_THREAD);
    int regionThreads = (int) OrbitalProperties.getLongGlobalProperty(PROP_REGION_THREAD, DFLT_REGION_THREAD);
    for (int i = 0; i < regionThreads; i++) {
      (new Thread(new RegionUpdater())).start();
    }
    for (int i = 0; i < historyThreads; i++) {
      (new Thread(new HistoryUpdater())).start();
    }
    // Start a single threaded market price updater
    (new Thread(new MarketPriceUpdater())).start();
    // The download doesn't terminate under normal circumstances. Ctrl-C to kill.
  }

  /**
   * Start periodic maintenance threads. There are four:
   * <ol>
   * <li>Refresh instrument map from CREST (normally once a day)
   * <li>Refresh region map from CREST (normally once a day)
   * <li>Unstick history download requests (by default, history threads have to finish within 10 minutes)
   * <li>Unstick region download requests (by default, region threads have to finish within 10 minutes)
   * </ol>
   */
  protected static void startMaintenanceThreads() {
    // Schedule instrument map updater to run on a timer
    (new Thread(new MaintenanceRunnable(OrbitalProperties.getLongGlobalProperty(PROP_REF_UPDATE_INTERVAL, DFLT_REF_UPDATE_INTERVAL), new Maintenance() {

      @Override
      public boolean performMaintenance() {
        return refreshInstrumentMap();
      }

    }, OrbitalProperties.getCurrentTime()))).start();
    // Schedule region map updater to run on a timer
    (new Thread(new MaintenanceRunnable(OrbitalProperties.getLongGlobalProperty(PROP_REF_UPDATE_INTERVAL, DFLT_REF_UPDATE_INTERVAL), new Maintenance() {

      @Override
      public boolean performMaintenance() {
        return refreshRegionMap();
      }

    }, OrbitalProperties.getCurrentTime()))).start();
    // Schedule stuck instrument cleanup to run on a timer
    (new Thread(
        new MaintenanceRunnable(
            OrbitalProperties.getLongGlobalProperty(PROP_STUCK_INSTRUMENT_UPDATE_INTERVAL, DFLT_STUCK_INSTRUMENT_UPDATE_INTERVAL), new Maintenance() {

              @Override
              public boolean performMaintenance() {
                long stuckDelay = OrbitalProperties.getLongGlobalProperty(PROP_STUCK_INSTRUMENT_UPDATE_INTERVAL, DFLT_STUCK_INSTRUMENT_UPDATE_INTERVAL);
                long threshold = OrbitalProperties.getCurrentTime() - stuckDelay;
                return unstickInstruments(threshold);
              }

            }, 0L))).start();
    // Schedule stuck region cleanup to run on a timer
    (new Thread(
        new MaintenanceRunnable(
            OrbitalProperties.getLongGlobalProperty(PROP_STUCK_REGION_UPDATE_INTERVAL, DFLT_STUCK_REGION_UPDATE_INTERVAL), new Maintenance() {

              @Override
              public boolean performMaintenance() {
                long stuckDelay = OrbitalProperties.getLongGlobalProperty(PROP_STUCK_REGION_UPDATE_INTERVAL, DFLT_STUCK_REGION_UPDATE_INTERVAL);
                long threshold = OrbitalProperties.getCurrentTime() - stuckDelay;
                return unstickRegions(threshold);
              }

            }, 0L))).start();
  }

  /**
   * Thread safe retrieval of current type map.
   * 
   * @return reference to type map
   */
  protected static Set<Integer> getTypeMap() {
    synchronized (Instrument.class) {
      return typeMap;
    }
  }

  /**
   * Thread safe retrieval of current region map.
   * 
   * @return reference to region map
   */
  protected static Set<Integer> getRegionMap() {
    synchronized (Region.class) {
      return regionMap;
    }
  }

  /**
   * Unschedule instruments which have been in the scheduled state for too long.
   * 
   * @param threshold
   *          max delay (in milliseconds) an instrument is allowed to remain scheduled
   * @return true if the unsticking pass succeeds, false otherwise
   */
  protected static boolean unstickInstruments(
                                              final long threshold) {
    log.info("Unsticking all instruments");
    try {
      EveKitMarketDataProvider.getFactory().runTransaction(new RunInVoidTransaction() {
        @Override
        public void run() throws Exception {
          for (Instrument delayed : Instrument.getDelayed(threshold)) {
            // Unschedule delayed instruments
            log.info("Unsticking (history) " + delayed.getTypeID() + " which has been scheduled since " + delayed.getScheduleTime());
            delayed.setScheduled(false);
            Instrument.update(delayed);
          }
        }
      });
    } catch (Exception e) {
      log.log(Level.SEVERE, "DB error updating instruments, aborting", e);
      return false;
    }
    log.info("Done unsticking instruments");
    return true;
  }

  /**
   * Unschedule regions which have been in the scheduled state for too long.
   * 
   * @param threshold
   *          max delay (in milliseconds) an instrument is allowed to remain scheduled
   * @return true if the unsticking pass succeeds, false otherwise
   */
  protected static boolean unstickRegions(
                                          final long threshold) {
    log.info("Unsticking all regions");
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
    log.info("Done unsticking regions");
    return true;
  }

  /**
   * Populate an integer set with the "id" field extracted from a JsonObject with a JsonArray field named "items".
   * 
   * @param target
   *          set of integers to be populated
   * @param data
   *          JsonObject containing a JsonArray filed named "items", where each "item" is a JsonObject with an integer "id" field.
   */
  protected static void fillReferenceSet(
                                         Set<Integer> target,
                                         JsonObject data) {
    JsonArray batch = data.getJsonArray("items");
    for (JsonObject next : batch.getValuesAs(JsonObject.class)) {
      target.add(next.getInt("id"));
    }
  }

  /**
   * Retrieve the latest set of regions from CREST.
   * 
   * @return a set of integers giving the latest regions from CREST.
   * @throws IOException
   *           if an error occurs retrieving the data from CREST.
   */
  protected static Set<Integer> populateRegionMap() throws IOException {
    Set<Integer> newRegionSet = new HashSet<>();
    URL root = new URL(OrbitalProperties.getGlobalProperty(PROP_CREST_ROOT, DFLT_CREST_ROOT));
    CRESTClient client = new CRESTClient(root);
    String baseHref = client.getData().getJsonObject("regions").getString("href");
    root = new URL(baseHref);
    CompletionService<JsonObject> urlResult = retrieveURLData(Collections.singletonList(root));
    JsonObject data;
    try {
      data = urlResult.take().get();
    } catch (Exception e) {
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
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    return newRegionSet;
  }

  /**
   * Retrieve the latest set of market types from CREST.
   * 
   * @return a set of integers giving the latest market types from CREST.
   * @throws IOException
   *           if an error occurs retrieving the data from CREST.
   */
  protected static Set<Integer> populateTypeMap() throws IOException {
    Set<Integer> newTypeSet = new HashSet<>();
    URL root = new URL(OrbitalProperties.getGlobalProperty(PROP_CREST_ROOT, DFLT_CREST_ROOT));
    CRESTClient client = new CRESTClient(root);
    String baseHref = client.getData().getJsonObject("marketTypes").getString("href");
    root = new URL(baseHref);
    CompletionService<JsonObject> urlResult = retrieveURLData(Collections.singletonList(root));
    JsonObject data;
    try {
      data = urlResult.take().get();
    } catch (Exception e) {
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
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    return newTypeSet;
  }

  /**
   * Refresh market types from CREST.
   * 
   * @return true on success, false if the refresh request failed.
   */
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
            Instrument newI = new Instrument(next, true, 0L);
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
    synchronized (Instrument.class) {
      typeMap = latestActive;
    }
    log.info("Instrument map refresh complete");
    return true;
  }

  /**
   * Refresh regions from CREST.
   * 
   * @return true on success, false if the refresh request failed.
   */
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
    synchronized (Region.class) {
      regionMap = latestActive;
    }
    log.info("Region map refresh complete");
    return true;
  }

  /**
   * Asynchronously retrieve CREST data. The caller receives a completion service which can be used to process the results as the complete.
   * 
   * @param targets
   *          the list of URL targets to retrieve.
   * @return a CompletionService which delivers the results as they arrive.
   */
  protected static CompletionService<JsonObject> retrieveURLData(
                                                                 List<URL> targets) {
    CompletionService<JsonObject> ecs = new ExecutorCompletionService<JsonObject>(urlRetrieverPool);
    for (final URL next : targets) {
      ecs.submit(new Callable<JsonObject>() {

        @Override
        public JsonObject call() throws Exception {
          IOException last = null;
          for (int tries = 0; tries < 3; tries++) {
            try {
              CRESTClient client = new CRESTClient(next);
              return client.getData();
            } catch (IOException e) {
              log.log(Level.WARNING, "Caught exception connecting to " + next + ", will retry shortly", e);
              last = e;
              // Sleep briefly before retry
              Thread.sleep(3000);
            }
          }
          assert last != null;
          throw last;
        }

      });

    }
    return ecs;
  }

  /**
   * Instances of this interface are invoked by maintenance threads on a schedule.
   */
  protected static interface Maintenance {
    public boolean performMaintenance();
  }

  /**
   * Periodically schedule maintenance work. This thread accepts an interval and invokes the maintenance task at least once per interval. The taks may be
   * invoked multiple times if an invocation fails.
   */
  protected static class MaintenanceRunnable implements Runnable {
    long        lastRun;
    long        updateInterval;
    Maintenance action;

    public MaintenanceRunnable(long interval, Maintenance task, long last) {
      updateInterval = interval;
      action = task;
      lastRun = last;
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
