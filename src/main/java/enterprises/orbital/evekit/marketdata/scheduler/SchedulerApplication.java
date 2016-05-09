package enterprises.orbital.evekit.marketdata.scheduler;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.core.Application;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.base.PersistentProperty;
import enterprises.orbital.db.ConnectionFactory.RunInVoidTransaction;
import enterprises.orbital.db.DBPropertyProvider;
import enterprises.orbital.evekit.marketdata.CRESTClient;
import enterprises.orbital.evekit.marketdata.EveKitMarketDataProvider;
import enterprises.orbital.evekit.marketdata.Instrument;
import enterprises.orbital.evekit.marketdata.Order;

public class SchedulerApplication extends Application {
  public static final Logger log                             = Logger.getLogger(SchedulerApplication.class.getName());
  // Property which holds the name of the persistence unit for properties
  public static final String PROP_APP_PATH                   = "enterprises.orbital.evekit.marketdata-scheduler.apppath";
  public static final String DEF_APP_PATH                    = "http://localhost/marketdata-scheduler";
  public static final String PROP_INSTRUMENT_UPDATE_INTERVAL = "enterprises.orbital.evekit.marketdata-scheduler.instUpdateInt";
  public static final long   DEF_INSTRUMENT_UPDATE_INTERVAL  = TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);
  public static final String PROP_STUCK_UPDATE_INTERVAL      = "enterprises.orbital.evekit.marketdata-scheduler.instStuckInt";
  public static final long   DEF_STUCK_UPDATE_INTERVAL       = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);
  public static final String PROP_ORDER_PROC_QUEUE_SIZE      = "enterprises.orbital.evekit.marketdata-scheduler.procQueueSize";
  public static final int    DEF_ORDER_PROC_QUEUE_SIZE       = 100;

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
    log.info("Instrument map refresh complete");
    return true;
  }

  protected static final int LOG_COUNTER_COUNTDOWN = 1000;
  protected static int       logCounter            = LOG_COUNTER_COUNTDOWN;

  protected static boolean updateLog() {
    synchronized (SchedulerApplication.class) {
      logCounter--;
      boolean out = logCounter <= 0;
      if (out) logCounter = LOG_COUNTER_COUNTDOWN;
      return out;
    }
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
          final List<Order> nextBatch = source.take();
          if (nextBatch.isEmpty()) continue;
          final int typeID = nextBatch.get(0).getTypeID();
          // Update time depends on when this item makes it to the front of the queue
          final long at = OrbitalProperties.getCurrentTime();
          boolean logit = updateLog();
          if (logit) log.info("Processing " + nextBatch.size() + " orders on (" + typeID + ")");
          // Extract regions we're updating in this batch
          final Set<Integer> regions = new HashSet<Integer>();
          for (Order next : nextBatch) {
            regions.add(next.getRegionID());
          }
          // Transact around the entire order load
          try {
            EveKitMarketDataProvider.getFactory().runTransaction(new RunInVoidTransaction() {
              @Override
              public void run() throws Exception {
                Map<Integer, Set<Long>> live = new HashMap<Integer, Set<Long>>();
                for (int regionID : regions) {
                  Set<Long> active = new HashSet<Long>();
                  active.addAll(Order.getLiveIDs(at, regionID, typeID));
                  live.put(regionID, active);
                }
                // Populate all orders
                for (Order next : nextBatch) {
                  int regionID = next.getRegionID();
                  // Record that this order is still live
                  live.get(regionID).remove(next.getOrderID());
                  // Construct new potential order
                  Order check = new Order(
                      regionID, typeID, next.getOrderID(), next.isBuy(), next.getIssued(), next.getPrice(), next.getVolumeEntered(), next.getMinVolume(),
                      next.getVolume(), next.getOrderRange(), next.getLocationID(), next.getDuration());
                  // Check whether we already know about this order
                  Order existing = Order.get(at, regionID, typeID, check.getOrderID());
                  // If the order exists and has changed, then evolve. Otherwise store the new order.
                  if (existing != null) {
                    // Existing, evolve if changed
                    if (!existing.equivalent(check)) {
                      // Evolve
                      existing.evolve(check, at);
                      Order.update(existing);
                      Order.update(check);
                    }
                  } else {
                    // New entity
                    check.setup(at);
                    Order.update(check);
                  }
                }
                // End of life orders no longer present in the book
                for (int regionID : live.keySet()) {
                  for (long orderID : live.get(regionID)) {
                    Order eol = Order.get(at, regionID, typeID, orderID);
                    if (eol != null) {
                      // NOTE: order may not longer exist if we're racing with another update
                      eol.evolve(null, at);
                      Order.update(eol);
                    }
                  }
                }
                // Update complete - release this instrument
                Instrument update = Instrument.get(typeID);
                update.setLastUpdate(at);
                update.setScheduled(false);
                Instrument.update(update);
              }
            });
          } catch (Exception e) {
            log.log(Level.SEVERE, "DB error storing order, failing: (" + typeID + ")", e);
          }

          long finish = OrbitalProperties.getCurrentTime();
          if (logit) log.info("Finished processing for (" + typeID + ") in " + TimeUnit.SECONDS.convert(finish - at, TimeUnit.MILLISECONDS) + " seconds");
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
