package enterprises.orbital.evekit.marketdata.scheduler;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.core.Application;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.base.PersistentProperty;
import enterprises.orbital.db.ConnectionFactory.RunInTransaction;
import enterprises.orbital.db.ConnectionFactory.RunInVoidTransaction;
import enterprises.orbital.db.DBPropertyProvider;
import enterprises.orbital.evekit.marketdata.CRESTClient;
import enterprises.orbital.evekit.marketdata.EveKitMarketDataProvider;
import enterprises.orbital.evekit.marketdata.Instrument;
import enterprises.orbital.evekit.marketdata.Order;
import io.prometheus.client.Histogram;

public class SchedulerApplication extends Application {
  public static final Logger    log                             = Logger.getLogger(SchedulerApplication.class.getName());
  // Property which holds the name of the persistence unit for properties
  public static final String    PROP_APP_PATH                   = "enterprises.orbital.evekit.marketdata-scheduler.apppath";
  public static final String    DEF_APP_PATH                    = "http://localhost/marketdata-scheduler";
  public static final String    PROP_INSTRUMENT_UPDATE_INTERVAL = "enterprises.orbital.evekit.marketdata-scheduler.instUpdateInt";
  public static final long      DEF_INSTRUMENT_UPDATE_INTERVAL  = TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);
  public static final String    PROP_STUCK_UPDATE_INTERVAL      = "enterprises.orbital.evekit.marketdata-scheduler.instStuckInt";
  public static final long      DEF_STUCK_UPDATE_INTERVAL       = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);
  public static final String    PROP_ORDER_PROC_QUEUE_SIZE      = "enterprises.orbital.evekit.marketdata-scheduler.procQueueSize";
  public static final int       DEF_ORDER_PROC_QUEUE_SIZE       = 100;
  public static final String    PROP_BOOK_DIR                   = "enterprises.orbital.evekit.marketdata-scheduler.bookDir";
  public static final String    DEF_BOOK_DIR                    = "";

  // Metrics
  public static final Histogram all_instrument_update_samples   = Histogram.build().name("all_instrument_update_delay_seconds")
      .help("Interval (seconds) between updates for all instruments.").linearBuckets(0, 60, 120).register();

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

  @SuppressWarnings("unchecked")
  public SchedulerApplication() throws IOException {
    // Populate properties
    OrbitalProperties.addPropertyFile("EveKitMarketdataScheduler.properties");
    // Sent persistence unit for properties
    PersistentProperty.setProvider(new DBPropertyProvider(OrbitalProperties.getGlobalProperty(EveKitMarketDataProvider.MARKETDATA_PU_PROP)));
    // Prepare processing queue
    createProcessingQueues();
    // Set agent if present
    String agent = OrbitalProperties.getGlobalProperty("enterprises.orbital.evekit.marketdata.crest.agent", null);
    if (agent != null) CRESTClient.setAgent(agent);
    // Schedule instrument map updater to run on a timer
    (new Thread(
        new MaintenanceRunnable(OrbitalProperties.getLongGlobalProperty(PROP_INSTRUMENT_UPDATE_INTERVAL, DEF_INSTRUMENT_UPDATE_INTERVAL), new Maintenance() {

          @Override
          public boolean performMaintenance() {
            return refreshInstrumentMap();
          }

        }))).start();
    // Schedule stuck instrument cleanup to run on a timer
    (new Thread(new MaintenanceRunnable(OrbitalProperties.getLongGlobalProperty(PROP_STUCK_UPDATE_INTERVAL, DEF_STUCK_UPDATE_INTERVAL), new Maintenance() {

      @Override
      public boolean performMaintenance() {
        return unstickInstruments();
      }

    }))).start();
    // Schedule order processing thread
    for (int i = 0; i < orderProcessingQueues.length; i++) {
      (new Thread(new OrderProcessor((ArrayBlockingQueue<List<Order>>) orderProcessingQueues[i]))).start();
    }
  }

  @Override
  public Set<Class<?>> getClasses() {
    Set<Class<?>> resources = new HashSet<Class<?>>();
    // Model APIresources
    resources.add(SchedulerWS.class);
    // Swagger additions
    resources.add(io.swagger.jaxrs.listing.ApiListingResource.class);
    resources.add(io.swagger.jaxrs.listing.SwaggerSerializers.class);
    // Return resource set
    return resources;
  }

  protected boolean unstickInstruments() {
    log.info("Checking for stuck instruments");
    long stuckDelay = OrbitalProperties.getLongGlobalProperty(PROP_STUCK_UPDATE_INTERVAL, DEF_STUCK_UPDATE_INTERVAL);
    // Retrieve current list of delayed instruments
    final long threshold = OrbitalProperties.getCurrentTime() - stuckDelay;
    synchronized (Instrument.class) {
      try {
        EveKitMarketDataProvider.getFactory().runTransaction(new RunInVoidTransaction() {
          @Override
          public void run() throws Exception {
            for (Instrument delayed : Instrument.getDelayed(threshold)) {
              // Unschedule delayed instruments
              log.info("Unsticking " + delayed.getTypeID() + " which has been scheduled since " + delayed.getScheduleTime());
              delayed.setScheduled(false);
              Instrument.update(delayed);
            }
          }
        });
      } catch (Exception e) {
        log.log(Level.SEVERE, "DB error updating instruments, aborting", e);
        return false;
      }
    }
    log.info("Stuck instruments check complete");
    return true;
  }

  protected boolean refreshInstrumentMap() {
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
      URL root = new URL(OrbitalProperties.getGlobalProperty(EveKitMarketDataProvider.CREST_ROOT_PROP, EveKitMarketDataProvider.CREST_ROOT_DEFAULT));
      CRESTClient client = new CRESTClient(root);
      client = client.down(client.getData().getJsonObject("marketTypes").getString("href"));
      do {
        JsonArray batch = client.getData().getJsonArray("items");
        for (JsonObject next : batch.getValuesAs(JsonObject.class)) {
          latestActive.add(next.getInt("id"));
        }
        if (!client.hasNext()) break;
        client = client.next();
      } while (true);
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
    }
    log.info("Instrument map refresh complete");
    return true;
  }

  public static Object[] orderProcessingQueues;

  // These functions encapsulate the current queue placement logic for order processors
  protected static void createProcessingQueues() {
    orderProcessingQueues = new Object[5];
    for (int i = 0; i < orderProcessingQueues.length; i++)
      orderProcessingQueues[i] = new ArrayBlockingQueue<List<Order>>(
          (int) OrbitalProperties.getLongGlobalProperty(PROP_ORDER_PROC_QUEUE_SIZE, DEF_ORDER_PROC_QUEUE_SIZE), true);
  }

  // This function encapsulates the queue placement logic for the order processor
  @SuppressWarnings("unchecked")
  public static void queueOrders(
                                 int typeID,
                                 List<Order> orderBlock)
    throws InterruptedException {
    switch (typeID % 10) {
    case 0:
    case 1:
      ((ArrayBlockingQueue<List<Order>>) orderProcessingQueues[0]).put(orderBlock);
      break;
    case 2:
    case 3:
      ((ArrayBlockingQueue<List<Order>>) orderProcessingQueues[1]).put(orderBlock);
      break;
    case 4:
    case 5:
      ((ArrayBlockingQueue<List<Order>>) orderProcessingQueues[2]).put(orderBlock);
      break;
    case 6:
    case 7:
      ((ArrayBlockingQueue<List<Order>>) orderProcessingQueues[3]).put(orderBlock);
      break;
    case 8:
    case 9:
      ((ArrayBlockingQueue<List<Order>>) orderProcessingQueues[4]).put(orderBlock);
      break;
    }
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
    out.format("%d,%b,%d,%.2f,%d,%d,%d,%s,%d,%d\n", o.getOrderID(), o.isBuy(), o.getIssued(), o.getPrice(), o.getVolumeEntered(), o.getMinVolume(),
               o.getVolume(), o.getOrderRange(), o.getLocationID(), o.getDuration());
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
    URI outURI = URI.create("jar:file:" + file);
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
          try {
            // Release instrument since we've finished
            long updateDelay = 0;
            synchronized (Instrument.class) {
              updateDelay = EveKitMarketDataProvider.getFactory().runTransaction(new RunInTransaction<Long>() {
                @Override
                public Long run() throws Exception {
                  // Update complete - release this instrument
                  Instrument update = Instrument.get(typeID);
                  long last = update.getLastUpdate();
                  update.setLastUpdate(at);
                  update.setScheduled(false);
                  Instrument.update(update);
                  return at - last;
                }
              });
            }
            // Store update delay metrics
            all_instrument_update_samples.observe(updateDelay / 1000);
          } catch (Exception e) {
            log.log(Level.SEVERE, "DB error storing order, failing: (" + typeID + ")", e);
          }
        } catch (InterruptedException e) {
          log.log(Level.INFO, "Break requested, exiting", e);
          System.exit(0);
        } catch (Exception f) {
          log.log(Level.SEVERE, "Fatal error caught in order processing loop, logging and attempting to continue", f);
        }
      }
    }
  }

}
