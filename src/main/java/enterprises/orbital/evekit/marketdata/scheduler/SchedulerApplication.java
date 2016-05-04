package enterprises.orbital.evekit.marketdata.scheduler;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
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

public class SchedulerApplication extends Application {
  public static final Logger log                             = Logger.getLogger(SchedulerApplication.class.getName());
  // Property which holds the name of the persistence unit for properties
  public static final String PROP_APP_PATH                   = "enterprises.orbital.evekit.marketdata-scheduler.apppath";
  public static final String DEF_APP_PATH                    = "http://localhost/marketdata-scheduler";
  public static final String PROP_INSTRUMENT_UPDATE_INTERVAL = "enterprises.orbital.evekit.marketdata-scheduler.instUpdateInt";
  public static final long   DEF_INSTRUMENT_UPDATE_INTERVAL  = TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);
  public static final String PROP_STUCK_UPDATE_INTERVAL      = "enterprises.orbital.evekit.marketdata-scheduler.instStuckInt";
  public static final long   DEF_STUCK_UPDATE_INTERVAL       = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

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

  public SchedulerApplication() throws IOException {
    // Populate properties
    OrbitalProperties.addPropertyFile("EveKitMarketdataScheduler.properties");
    // Sent persistence unit for properties
    PersistentProperty.setProvider(new DBPropertyProvider(OrbitalProperties.getGlobalProperty(EveKitMarketDataProvider.MARKETDATA_PU_PROP)));
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
    long threshold = OrbitalProperties.getCurrentTime() - stuckDelay;
    try {
      for (Instrument delayed : Instrument.getDelayed(threshold)) {
        // Unschedule delayed instruments
        log.info("Unsticking " + delayed.getTypeID() + " which has been scheduled since " + delayed.getScheduleTime());
        delayed.setScheduled(false);
        Instrument.update(delayed);
      }
    } catch (Exception e) {
      log.log(Level.SEVERE, "DB error while unsticking instruments, aborting", e);
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

}
