package enterprises.orbital.evekit.marketdata.scheduler;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.base.PersistentProperty;
import enterprises.orbital.db.ConnectionFactory.RunInVoidTransaction;
import enterprises.orbital.evekit.marketdata.EveKitMarketDataProvider;
import enterprises.orbital.evekit.marketdata.Instrument;
import enterprises.orbital.evekit.marketdata.Order;
import io.prometheus.client.Histogram;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

/**
 * API for marketdata update scheduler.
 */
@Path("/ws/v1/scheduler")
@Consumes({
    "application/json"
})
@Produces({
    "application/json"
})
@Api(
    tags = {
        "Scheduler"
    },
    produces = "application/json",
    consumes = "application/json")
public class SchedulerWS {
  private static final Logger   log                                = Logger.getLogger(SchedulerWS.class.getName());
  public static final String    PROP_MIN_SCHED_INTERVAL            = "enterprises.orbital.evekit.marketdata.scheduler.minSchedInterval";
  public static final long      DEF_MIN_SCHED_INTERVAL             = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

  // public static final Histogram instrument_web_request_samples = Histogram.build().name("instrument_web_request_delay_seconds")
  // .help("Interval (seconds) between updates for an instrument.").labelNames("type_id").linearBuckets(0, 60, 120).register();
  public static final Histogram all_instrument_web_request_samples = Histogram.build().name("all_instrument_web_request_delay_seconds")
      .help("Interval (seconds) between updates for all instruments.").linearBuckets(0, 60, 120).register();

  @Path("/takenext")
  @GET
  @ApiOperation(
      value = "Get next instrument for which marketdata should be updated.  Caller is assigned the instrument and is responsible for releasing it when the update is complete.")
  @ApiResponses(
      value = {
          @ApiResponse(
              code = 200,
              message = "Next instrument to be updated",
              response = Instrument.class),
          @ApiResponse(
              code = 404,
              message = "No instrument ready to be scheduled , try again later."),
          @ApiResponse(
              code = 500,
              message = "Internal error"),
      })
  public Response takeNext(
                           @Context HttpServletRequest request) {
    long interval = PersistentProperty.getLongPropertyWithFallback(PROP_MIN_SCHED_INTERVAL, DEF_MIN_SCHED_INTERVAL);
    Instrument next;
    try {
      next = Instrument.takeNextScheduled(interval);
    } catch (Exception e) {
      log.log(Level.SEVERE, "DB error retrieving instrument, failing", e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).build();
    }
    if (next == null) return Response.status(Status.NOT_FOUND).build();
    return Response.ok().entity(next).build();
  }

  @Path("/storeAll")
  @POST
  @ApiOperation(
      value = "Populate a complete set of orders for all regions for a given type.  Clean any orders that aren't listed.  Release the type when finished")
  @ApiResponses(
      value = {
          @ApiResponse(
              code = 200,
              message = "Population successful"),
          @ApiResponse(
              code = 500,
              message = "Internal error"),
      })
  public Response storeAll(
                           @Context HttpServletRequest request,
                           @QueryParam("typeid") @ApiParam(
                               name = "typeid",
                               required = true,
                               value = "Type ID of order") final int typeID,
                           @ApiParam(
                               name = "orders",
                               required = true,
                               value = "Orders to populate") final List<Order> orders) {
    final long at = OrbitalProperties.getCurrentTime();
    if (orders.size() > 0) {
      // Queue up orders for processing. We'll block if the queue is backlogged.
      try {
        SchedulerApplication.queueOrders(typeID, orders);
      } catch (InterruptedException e) {
        log.log(Level.INFO, "Break requested, exiting", e);
        System.exit(0);
      }
    } else {
      // Release instrument now since no orders were queued
      try {
        EveKitMarketDataProvider.getFactory().runTransaction(new RunInVoidTransaction() {
          @Override
          public void run() throws Exception {
            // Update complete - release this instrument
            Instrument update = Instrument.get(typeID);
            update.setLastUpdate(at);
            update.setScheduled(false);
            Instrument.update(update);
          }
        });
        long updateDelay = OrbitalProperties.getCurrentTime() - at;
        // SchedulerApplication.instrument_update_samples.labels(String.valueOf(typeID)).observe(updateDelay / 1000);
        SchedulerApplication.all_instrument_update_samples.observe(updateDelay / 1000);
      } catch (Exception e) {
        log.log(Level.SEVERE, "DB error storing order, failing: (" + typeID + ")", e);
        return Response.status(Status.INTERNAL_SERVER_ERROR).build();
      }
    }
    long updateDelay = OrbitalProperties.getCurrentTime() - at;
    // instrument_web_request_samples.labels(String.valueOf(typeID)).observe(updateDelay / 1000);
    all_instrument_web_request_samples.observe(updateDelay / 1000);
    // Order accepted
    return Response.ok().build();
  }

}
