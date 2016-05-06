package enterprises.orbital.evekit.marketdata.scheduler;

import java.math.BigDecimal;
import java.math.RoundingMode;
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
  private static final Logger log                     = Logger.getLogger(SchedulerWS.class.getName());
  public static final String  PROP_MIN_SCHED_INTERVAL = "enterprises.orbital.evekit.marketdata.scheduler.minSchedInterval";
  public static final long    DEF_MIN_SCHED_INTERVAL  = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

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

  @Path("/store")
  @POST
  @ApiOperation(
      value = "Populate an order for an instrument.")
  @ApiResponses(
      value = {
          @ApiResponse(
              code = 200,
              message = "Population successful"),
          @ApiResponse(
              code = 500,
              message = "Internal error"),
      })
  public Response storeOrder(
                             @Context HttpServletRequest request,
                             @QueryParam("regionid") @ApiParam(
                                 name = "regionid",
                                 required = true,
                                 value = "Region where order is located") final int regionID,
                             @QueryParam("typeid") @ApiParam(
                                 name = "typeid",
                                 required = true,
                                 value = "Type ID of order") final int typeID,
                             @QueryParam("orderid") @ApiParam(
                                 name = "orderid",
                                 required = true,
                                 value = "Order ID") final long orderID,
                             @QueryParam("buy") @ApiParam(
                                 name = "buy",
                                 required = true,
                                 value = "True if the order is a buy, false otherwise") final boolean buy,
                             @QueryParam("issued") @ApiParam(
                                 name = "issued",
                                 required = true,
                                 value = "Order issue date (milliseconds UTC)") final long issued,
                             @QueryParam("price") @ApiParam(
                                 name = "price",
                                 required = true,
                                 value = "Order price (converted to big decimal)") final String price,
                             @QueryParam("volumeentered") @ApiParam(
                                 name = "volumeentered",
                                 required = true,
                                 value = "Order volume entered") final int volumeEntered,
                             @QueryParam("minvolume") @ApiParam(
                                 name = "minvolume",
                                 required = true,
                                 value = "Minimum order volume") final int minVolume,
                             @QueryParam("volume") @ApiParam(
                                 name = "volume",
                                 required = true,
                                 value = "Order volume") final int volume,
                             @QueryParam("range") @ApiParam(
                                 name = "range",
                                 required = true,
                                 value = "Order range") final String range,
                             @QueryParam("locationid") @ApiParam(
                                 name = "locationid",
                                 required = true,
                                 value = "Location where order was entered") final long locationID,
                             @QueryParam("duration") @ApiParam(
                                 name = "duration",
                                 required = true,
                                 value = "Order duration") final int duration) {
    final BigDecimal priceValue = BigDecimal.valueOf(Double.valueOf(price).doubleValue()).setScale(2, RoundingMode.HALF_UP);
    final long at = OrbitalProperties.getCurrentTime();
    // Construct new potential order
    final Order check = new Order(regionID, typeID, orderID, buy, issued, priceValue, volumeEntered, minVolume, volume, range, locationID, duration);
    try {
      EveKitMarketDataProvider.getFactory().runTransaction(new RunInVoidTransaction() {
        @Override
        public void run() throws Exception {
          // Check whether we already know about this order
          Order existing = Order.get(at, regionID, typeID, orderID);
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
      });
    } catch (Exception e) {
      log.log(Level.SEVERE, "DB error storing order, failing", e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).build();
    }
    // Order accepted
    return Response.ok().build();
  }

  @Path("/close")
  @POST
  @ApiOperation(
      value = "EOL orders which are no longer active")
  @ApiResponses(
      value = {
          @ApiResponse(
              code = 200,
              message = "Orders successfully end of lifed"),
          @ApiResponse(
              code = 500,
              message = "Internal error"),
      })
  public Response closeOrders(
                              @Context HttpServletRequest request,
                              @QueryParam("regionid") @ApiParam(
                                  name = "regionid",
                                  required = true,
                                  value = "Region where order is located") final int regionID,
                              @QueryParam("typeid") @ApiParam(
                                  name = "typeid",
                                  required = true,
                                  value = "Type ID of order") final int typeID,
                              @ApiParam(
                                  name = "orders",
                                  required = true,
                                  value = "Orders to be end of lifed") final List<Long> orders) {
    try {
      final long time = OrbitalProperties.getCurrentTime();
      EveKitMarketDataProvider.getFactory().runTransaction(new RunInVoidTransaction() {
        @Override
        public void run() throws Exception {
          // End of life orders no longer present in the book
          for (Long orderID : orders) {
            Order eol = Order.get(time, regionID, typeID, orderID);
            if (eol != null) {
              // NOTE: order may not longer exist if we're racing with another update
              eol.evolve(null, time);
              Order.update(eol);
            }
          }
        }
      });
    } catch (Exception e) {
      log.log(Level.SEVERE, "DB error closing orders, failing", e);
      return Response.status(Status.INTERNAL_SERVER_ERROR).build();
    }
    // Orders closed
    return Response.ok().build();
  }
}
