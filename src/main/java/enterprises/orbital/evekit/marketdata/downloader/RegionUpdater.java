package enterprises.orbital.evekit.marketdata.downloader;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.CompletionService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import javax.json.JsonArray;
import javax.json.JsonObject;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.base.PersistentProperty;
import enterprises.orbital.db.ConnectionFactory.RunInTransaction;
import enterprises.orbital.evekit.marketdata.model.EveKitMarketDataProvider;
import enterprises.orbital.evekit.marketdata.model.Order;
import enterprises.orbital.evekit.marketdata.model.Region;

public class RegionUpdater implements Runnable {
  private static final Logger log = Logger.getLogger(RegionUpdater.class.getName());

  /**
   * Main region update processing loop.
   */
  @Override
  public void run() {
    while (true) {
      try {
        log.info("Starting region updater thread");
        long interval = PersistentProperty.getLongPropertyWithFallback(MarketDownloader.PROP_MIN_REGION_SCHED_INTERVAL,
                                                                       MarketDownloader.DFLT_MIN_REGION_SCHED_INTERVAL);
        // Spin forever updating regions
        while (true) {
          // Attempt to retrieve region to update, otherwise loop and sleep
          Region region = Region.takeNextScheduled(interval);
          if (region == null) {
            log.info("No region available for update, waiting");
            Thread.sleep(TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS));
            continue;
          }
          // Start population for this region
          log.fine("Starting population on: " + region);
          long startPopTime = OrbitalProperties.getCurrentTime();
          final int regionID = region.getRegionID();
          // Download region data
          try {
            Set<Integer> typeMap = MarketDownloader.getTypeMap();
            populateRegion(regionID, typeMap);
            log.fine("Populated (" + regionID + ")");
          } catch (IOException e) {
            // On failed, we log the the failure and then release the instrument.
            // We'll try again after the update timer expires
            log.log(Level.SEVERE, "Failed to populate (" + regionID + ")", e);
          }
          // Release region
          final long at = OrbitalProperties.getCurrentTime();
          try {
            long updateDelay = EveKitMarketDataProvider.getFactory().runTransaction(new RunInTransaction<Long>() {
              @Override
              public Long run() throws Exception {
                // Update complete - release this region
                Region update = Region.get(regionID);
                long last = update.getLastUpdate();
                update.setLastUpdate(at);
                update.setScheduled(false);
                Region.update(update);
                return at - last;
              }
            });
            MarketDownloader.all_region_update_samples.observe(updateDelay / 1000);
          } catch (Exception e) {
            log.log(Level.SEVERE, "DB error releasing region, failing: (" + regionID + ")", e);
          }
          // Record update time
          long endPopTime = OrbitalProperties.getCurrentTime();
          log.info("Completed population of region " + regionID + " in time: " + TimeUnit.SECONDS.convert(endPopTime - startPopTime, TimeUnit.MILLISECONDS)
              + " s");
        }
      } catch (Exception e) {
        log.log(Level.SEVERE, "Unexpected error in populator thread, attempting to restart", e);
      }
    }
  }

  /**
   * Download and store data for the given region.
   * 
   * @param region
   *          the ID of the region to download.
   * @param typeMap
   *          type filter. If a type in the region is not a member of this set, then we ignore it.
   * @throws IOException
   *           if an error occurs downloading the requested region.
   */
  protected void populateRegion(
                                final int region,
                                final Set<Integer> typeMap)
    throws IOException {
    // Setup for region retrieval
    String regionHref = OrbitalProperties.getGlobalProperty(MarketDownloader.PROP_CREST_ROOT, MarketDownloader.DFLT_CREST_ROOT) + "market/" + region
        + "/orders/all/";
    // Retrieve and populate orders
    long startPopTime = OrbitalProperties.getCurrentTime();
    List<Order> toStore = new ArrayList<Order>();
    // Retrieve first page and process
    URL root = new URL(regionHref);
    CompletionService<JsonObject> urlResult = MarketDownloader.retrieveURLData(Collections.singletonList(root));
    JsonObject data;
    try {
      data = urlResult.take().get();
    } catch (Exception e) {
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
      urlResult = MarketDownloader.retrieveURLData(targets);
      // Process all downloads
      try {
        for (i = 2; i <= pageCount; i++) {
          data = urlResult.take().get();
          popRegionOrdersFromResult(region, typeMap, data, toStore);
          log.info("Accumulated " + toStore.size() + " results for region " + region);
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    log.info("Sending " + toStore.size() + " results for region " + region);
    writeRegionSnap(OrbitalProperties.getCurrentTime(), region, toStore);
    log.info("Stored " + toStore.size() + " results for region " + region);
    long endDownloadTime = OrbitalProperties.getCurrentTime();
    MarketDownloader.all_region_download_samples.observe((endDownloadTime - startPopTime) / 1000);
  }

  /**
   * Extract orders from a JSON region order download.
   * 
   * @param region
   *          region we downloaded.
   * @param typeFilter
   *          filter used to determine market types to keep.
   * @param data
   *          JSON object retrieved from download.
   * @param storage
   *          list where orders will be stored
   * @throws IOException
   *           if an error occurs while populating orders
   */
  protected void popRegionOrdersFromResult(
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

  /**
   * Extract an Order object from a JSON order representation.
   * 
   * @param regionID
   *          region which contained order.
   * @param order
   *          JSON representation of order
   * @return Order instance populated with download result
   * @throws ParseException
   *           if an error occurs parsing JSON
   */
  protected static Order convertRegionOrder(
                                            int regionID,
                                            JsonObject order)
    throws ParseException {
    // Extract fields
    int typeID = order.getJsonNumber("type").intValue();
    long orderID = order.getJsonNumber("id").longValue();
    boolean buy = order.getBoolean("buy");
    long issued = MarketDownloader.dateFormat.get().parse(order.getString("issued")).getTime();
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

  /**
   * Write order data to a region file.
   * 
   * @param at
   *          the timestamp for the new file.
   * @param regionID
   *          the region for which orders are being recorded.
   * @param orders
   *          the set of orders to store.
   * @throws IOException
   *           if an error occurs writing the file.
   */
  protected static void writeRegionSnap(
                                        long at,
                                        int regionID,
                                        List<Order> orders)
    throws IOException {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    String regionFileName = String.format("region_%d_%s.gz", at, formatter.format(new Date(at)));
    Path dir = Paths.get(OrbitalProperties.getGlobalProperty(MarketDownloader.PROP_REGION_DIR, MarketDownloader.DFLT_REGION_DIR), "regions",
                         String.valueOf(regionID));
    Files.createDirectories(dir);
    Path file = Paths.get(OrbitalProperties.getGlobalProperty(MarketDownloader.PROP_REGION_DIR, MarketDownloader.DFLT_REGION_DIR), "regions",
                          String.valueOf(regionID), regionFileName);
    try (PrintWriter snapOut = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file.toFile()))))) {
      // Write header
      snapOut.format("%d\n", orders.size());
      // Dump all orders
      for (Order next : orders) {
        writeOrder(next, snapOut);
      }
    }
  }

  /**
   * Dump an order to an output source.
   * 
   * @param o
   *          the Order to dump.
   * @param out
   *          the output source where the order will be written.
   */
  protected static void writeOrder(
                                   Order o,
                                   PrintWriter out) {
    out.format("%d,%d,%d,%b,%d,%.2f,%d,%d,%d,%s,%d,%d\n", o.getRegionID(), o.getTypeID(), o.getOrderID(), o.isBuy(), o.getIssued(), o.getPrice(),
               o.getVolumeEntered(), o.getMinVolume(), o.getVolume(), o.getOrderRange(), o.getLocationID(), o.getDuration());
  }

}