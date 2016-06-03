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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import enterprises.orbital.evekit.marketdata.model.Instrument;
import enterprises.orbital.evekit.marketdata.model.MarketHistory;

/**
 * Download and store market history updates. Market history is available per-type, per-region, and includes around 424 days of history. The market history file
 * format is UTF-8 with the first line consisting of the number of entries, and the remaining lines consisting of one MarketHistory entry per line. With about
 * 13000 types on the market, and 100 regions, the size of market history could be as large as 1.3m files. In practice, with filtering and lack of market
 * history in some regions, the actual number of files is usually in the small 100's of thousands.
 * 
 * Given the large cache time for market history, typically you'd only download a snapshot once for each day.
 */
public class HistoryUpdater implements Runnable {
  private static final Logger log = Logger.getLogger(HistoryUpdater.class.getName());

  @Override
  public void run() {
    while (true) {
      try {
        log.info("Starting history updater thread");
        // Spin forever updating market history
        long interval = PersistentProperty.getLongPropertyWithFallback(MarketDownloader.PROP_MIN_HISTORY_SCHED_INTERVAL,
                                                                       MarketDownloader.DFLT_MIN_HISTORY_SCHED_INTERVAL);
        while (true) {
          // Attempt to retrieve instrument to update, otherwise loop and sleep
          // Note that type filtering here occurs in the database, which we assume will not allow us to schedule inactive instruments.
          Instrument next = null;
          synchronized (Instrument.class) {
            next = Instrument.takeNextScheduled(interval);
          }
          if (next == null) {
            log.info("No instrument available for update, waiting");
            Thread.sleep(TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS));
            continue;
          }
          // Start history population
          log.fine("Starting history population on: " + next);
          final long startPopTime = OrbitalProperties.getCurrentTime();
          Set<Integer> regionMap = MarketDownloader.getRegionMap();
          final int typeID = next.getTypeID();
          // Collect all history updates
          List<MarketHistory> toStore = new ArrayList<MarketHistory>();
          // Populate each region for this type
          for (int regionID : regionMap) {
            try {
              toStore.addAll(populateHistory(regionID, typeID));
              log.fine("Populated (" + regionID + ", " + typeID + ")");
            } catch (IOException e) {
              log.log(Level.SEVERE, "Failed to populate (" + regionID + ", " + typeID + ")", e);
            }
          }
          long endDownloadTime = OrbitalProperties.getCurrentTime();
          MarketDownloader.all_history_download_samples.observe((endDownloadTime - startPopTime) / 1000);
          // Store history if we retrieved data
          if (!toStore.isEmpty()) processBatch(typeID, toStore);
          // Release instrument
          synchronized (Instrument.class) {
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
                    update.setLastUpdate(startPopTime);
                    update.setScheduled(false);
                    Instrument.update(update);
                    return startPopTime - last;
                  }
                });
              }
              // Store update delay metrics
              MarketDownloader.all_history_update_samples.observe(updateDelay / 1000);
            } catch (Exception e) {
              log.log(Level.SEVERE, "DB error storing order, failing: (" + typeID + ")", e);
            }
          }
          // Record update time
          long endPopTime = OrbitalProperties.getCurrentTime();
          log.info("Completed population of type " + typeID + " in time: " + TimeUnit.SECONDS.convert(endPopTime - startPopTime, TimeUnit.MILLISECONDS) + " s");
        }
      } catch (Exception e) {
        log.log(Level.SEVERE, "Unexpected error in populator thread, attempting to restart", e);
      }
    }
  }

  /**
   * Retrieve market history for the given region and type. Return appropriate MarketOrders.
   * 
   * @param region
   *          region to populate
   * @param type
   *          type to populate
   * @return list of MarketHistory to store
   * @throws IOException
   *           if an error occurs retrieving history
   */
  protected static List<MarketHistory> populateHistory(
                                                       final int region,
                                                       final int type)
    throws IOException {
    // Setup for history retrieval
    // Href currently NOT discoverable from CREST root so we hand construct, e.g. https://crest-tq.eveonline.com/market/10000002/types/34/history/
    String historyHref = OrbitalProperties.getGlobalProperty(MarketDownloader.PROP_CREST_ROOT, MarketDownloader.DFLT_CREST_ROOT) + "market/" + region
        + "/types/" + type + "/history/";
    // Retrieve and populate first page of history
    URL root = new URL(historyHref);
    List<MarketHistory> toStore = new ArrayList<MarketHistory>();
    CompletionService<JsonObject> urlResult = MarketDownloader.retrieveURLData(Collections.singletonList(root));
    JsonObject data;
    try {
      data = urlResult.take().get();
    } catch (Exception e) {
      throw new IOException(e);
    }
    // Store retrieved results in array
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
      urlResult = MarketDownloader.retrieveURLData(targets);
      // Process all downloads
      try {
        for (i = 2; i <= pageCount; i++) {
          data = urlResult.take().get();
          // Store retrieved results in array
          popMarketHistoryFromResult(region, type, data, toStore);
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    // Return result
    return toStore;
  }

  /**
   * Extract MarketHistory entries from JSON and store.
   * 
   * @param region
   *          region where market history was retrieved
   * @param type
   *          type of market history retrieved
   * @param data
   *          JSON holding retrieval results
   * @param storage
   *          list where MarketHistory objects should be added
   * @throws IOException
   *           if an error occurs while parsing JSON
   */
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

  /**
   * Convert a JSON representation of market history to a MarketHistory object.
   * 
   * @param regionID
   *          region from which market history was retrieved
   * @param typeID
   *          type of market history retrieved
   * @param history
   *          JSON holding market history value
   * @return a MarketHistory object
   * @throws ParseException
   *           if an error occurs while parsing JSON
   */
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
    long date = MarketDownloader.dateFormat.get().parse(history.getString("date")).getTime();
    // Filter out garbage dates, this happens in the results every once in a while
    long dateLowerBound = OrbitalProperties.getCurrentTime() - TimeUnit.MILLISECONDS.convert(700, TimeUnit.DAYS);
    long dateUpperBound = OrbitalProperties.getCurrentTime();
    if (date < dateLowerBound || date > dateUpperBound) { throw new ParseException("History has obviously bad date: " + date, 0); }
    MarketHistory result = new MarketHistory(typeID, regionID, orderCount, lowPrice, highPrice, avgPrice, volume, date);
    return result;
  }

  /**
   * Dump market history into per-region files for the given type.
   * 
   * @param typeID
   *          type of market history to dump.
   * @param nextBatch
   *          the list of MarketHistory objects to dump.
   */
  protected void processBatch(
                              final int typeID,
                              List<MarketHistory> nextBatch) {
    try {
      final long at = OrbitalProperties.getCurrentTime();
      // Sort market history by region
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
      // Write one file per region
      for (int regionID : regionMap.keySet()) {
        writeBulkHistorySnap(at, typeID, regionID, regionMap.get(regionID));
      }
    } catch (Exception f) {
      log.log(Level.SEVERE, "Fatal error caught in history processing loop, logging and attempting to continue", f);
    }
  }

  /**
   * Write market history file for a given type and region.
   * 
   * @param at
   *          snapshot time for this file.
   * @param typeID
   *          type for which market history will be written
   * @param regionID
   *          region for which market history will be written
   * @param history
   *          list of MarketHistory objects to be written
   * @throws IOException
   *           if an error occurs writing the output file
   */
  protected static void writeBulkHistorySnap(
                                             long at,
                                             int typeID,
                                             int regionID,
                                             List<MarketHistory> history)
    throws IOException {
    // History files are written to a file with name:
    //
    // <root>/history/<typeID>/history_<snapTime>_<regionID>_<snapDateString>.gz
    //
    // Within the file, the format is:
    // total number of market history entries
    // first market history
    // second market history
    //
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    String historyFileName = String.format("history_%d_%d_%s.gz", at, regionID, formatter.format(new Date(at)));
    Path dir = Paths.get(OrbitalProperties.getGlobalProperty(MarketDownloader.PROP_HISTORY_DIR, MarketDownloader.DFLT_HISTORY_DIR), "history",
                         String.valueOf(typeID));
    Files.createDirectories(dir);
    Path file = Paths.get(OrbitalProperties.getGlobalProperty(MarketDownloader.PROP_HISTORY_DIR, MarketDownloader.DFLT_HISTORY_DIR), "history",
                          String.valueOf(typeID), historyFileName);
    try (PrintWriter snapOut = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file.toFile()))))) {
      // Write header
      snapOut.format("%d\n", history.size());
      // Dump all history
      for (MarketHistory next : history) {
        writeHistory(next, snapOut);
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