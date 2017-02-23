package enterprises.orbital.evekit.marketdata.downloader;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.base.PersistentProperty;
import enterprises.orbital.eve.esi.client.api.MarketApi;
import enterprises.orbital.eve.esi.client.invoker.ApiException;
import enterprises.orbital.eve.esi.client.model.GetMarketsPrices200Ok;
import enterprises.orbital.evekit.marketdata.model.MarketPrice;

/**
 * Download and store market price updates. Market prices are provided as a single bulk update from CCP with all types stored in the update. There are no
 * per-region updates, so all we need to process and store is this one update. Market prices are updated once a day.
 */
public class MarketPriceUpdater implements Runnable {
  private static final Logger log = Logger.getLogger(MarketPriceUpdater.class.getName());

  @Override
  public void run() {
    while (true) {
      long interval = PersistentProperty.getLongPropertyWithFallback(MarketDownloader.PROP_MIN_PRICE_SCHED_INTERVAL,
                                                                     MarketDownloader.DFLT_MIN_PRICE_SCHED_INTERVAL);
      long lastUpdate = 0;
      try {
        log.info("Starting market price updater thread");
        // Spin forever updating market prices
        while (true) {
          // Delay until update time
          long now = OrbitalProperties.getCurrentTime();
          if (lastUpdate + interval > now) Thread.sleep(lastUpdate + interval - now);
          // There's no instrument to take here since the update consists of one single file.
          // As a result, we simply try to update each time the scheduling interval has passed.
          log.fine("Starting market price population");
          final long startPopTime = OrbitalProperties.getCurrentTime();
          // Collect prices
          List<MarketPrice> toStore = new ArrayList<MarketPrice>();
          try {
            toStore.addAll(populatePrice());
          } catch (IOException e) {
            log.log(Level.SEVERE, "Failed to populate prices", e);
          }
          long endDownloadTime = OrbitalProperties.getCurrentTime();
          MarketDownloader.all_price_download_samples.observe((endDownloadTime - startPopTime) / 1000);
          // Store prices if we retrieved data
          if (!toStore.isEmpty()) processBatch(toStore);
          // Record update time
          long endPopTime = OrbitalProperties.getCurrentTime();
          log.info("Completed population of prices in time: " + TimeUnit.SECONDS.convert(endPopTime - startPopTime, TimeUnit.MILLISECONDS) + " s");
          lastUpdate = endPopTime;
        }
      } catch (Exception e) {
        log.log(Level.SEVERE, "Unexpected error in populator thread, attempting to restart", e);
      }
    }
  }

  /**
   * Retrieve market prices. Return appropriate MarketPrices.
   * 
   * @return list of MarketPrice to store
   * @throws IOException
   *           if an error occurs retrieving prices
   */
  protected static List<MarketPrice> populatePrice() throws IOException {
    // Create ESI client and retrieve results
    long now = OrbitalProperties.getCurrentTime();
    MarketApi client = new MarketApi();
    List<GetMarketsPrices200Ok> prices;
    try {
      prices = client.getMarketsPrices("tranquility");
    } catch (ApiException e) {
      log.log(Level.WARNING, "Error retrieving market prices", e);
      throw new IOException(e);
    }
    List<MarketPrice> toStore = new ArrayList<MarketPrice>();
    // Store retrieved results in array
    popMarketPriceFromResult(now, prices, toStore);
    // Return result
    return toStore;
  }

  /**
   * Extract MarketPrice entries from result object for storage.
   * 
   * @param at
   *          time when prices were retrieved
   * @param prices
   *          returned prices from ESI call.
   * @param storage
   *          list where MarketPrice objects should be added
   * @throws IOException
   *           if an error occurs while parsing JSON
   */
  protected static void popMarketPriceFromResult(
                                                 long at,
                                                 List<GetMarketsPrices200Ok> prices,
                                                 List<MarketPrice> storage)
    throws IOException {
    for (enterprises.orbital.eve.esi.client.model.GetMarketsPrices200Ok nextPrice : prices) {
      storage.add(convertMarketPrice(at, nextPrice));
    }
  }

  /**
   * Convert ESI result into a MarketPrice object.
   * 
   * @param at
   *          Time when price was retrieved.
   * @param price
   *          Swagger result object
   * @return a MarketPrice object
   */
  protected static MarketPrice convertMarketPrice(
                                                  long at,
                                                  enterprises.orbital.eve.esi.client.model.GetMarketsPrices200Ok price) {
    // Extract fields
    return new MarketPrice(
        price.getTypeId(), BigDecimal.valueOf(price.getAdjustedPrice().doubleValue()).setScale(2, RoundingMode.HALF_UP),
        BigDecimal.valueOf(price.getAveragePrice().doubleValue()).setScale(2, RoundingMode.HALF_UP), at);
  }

  /**
   * Dump market prices into a file.
   * 
   * @param nextBatch
   *          the list of MarketPrice objects to dump.
   */
  protected void processBatch(
                              List<MarketPrice> nextBatch) {
    try {
      final long at = OrbitalProperties.getCurrentTime();
      writeBulkPriceSnap(at, nextBatch);
    } catch (Exception f) {
      log.log(Level.SEVERE, "Fatal error caught in price processing loop, logging and attempting to continue", f);
    }
  }

  /**
   * Write market price file for a given type.
   * 
   * @param at
   *          snapshot time for this file.
   * @param prices
   *          list of MarketPrices objects to be written
   * @throws IOException
   *           if an error occurs writing the output file
   */
  protected static void writeBulkPriceSnap(
                                           long at,
                                           List<MarketPrice> prices)
    throws IOException {
    // Price files are written to a file with name:
    //
    // <root>/price/price_<snapTime>_<snapDateString>.gz
    //
    // Within the file, the format is:
    // total number of market price entries
    // first market price
    // second market price
    // ...
    //
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    String priceFileName = String.format("price_%d_%s.gz", at, formatter.format(new Date(at)));
    Path dir = Paths.get(OrbitalProperties.getGlobalProperty(MarketDownloader.PROP_PRICE_DIR, MarketDownloader.DFLT_PRICE_DIR), "price");
    Files.createDirectories(dir);
    Path file = Paths.get(OrbitalProperties.getGlobalProperty(MarketDownloader.PROP_PRICE_DIR, MarketDownloader.DFLT_PRICE_DIR), "price", priceFileName);
    try (PrintWriter snapOut = new PrintWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file.toFile()))))) {
      // Write header
      snapOut.format("%d\n", prices.size());
      // Dump all prices
      for (MarketPrice next : prices) {
        writeMarketPrice(next, snapOut);
      }
    }
  }

  protected static void writeMarketPrice(
                                         MarketPrice p,
                                         PrintWriter out) {
    out.format("%d,%.2f,%.2f,%d\n", p.getTypeID(), p.getAdjustedPrice(), p.getAveragePrice(), p.getDate());
  }

}