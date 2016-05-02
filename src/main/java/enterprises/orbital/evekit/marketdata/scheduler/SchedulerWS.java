package enterprises.orbital.evekit.marketdata.scheduler;

import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import enterprises.orbital.base.PersistentProperty;
import enterprises.orbital.evekit.marketdata.Instrument;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
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
  public static final String PROP_MIN_SCHED_INTERVAL = "enterprises.orbital.evekit.marketdata.scheduler.minSchedInterval";
  public static final long   DEF_MIN_SCHED_INTERVAL  = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

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
      })
  public Response takeNext(
                           @Context HttpServletRequest request) {
    long interval = PersistentProperty.getLongPropertyWithFallback(PROP_MIN_SCHED_INTERVAL, DEF_MIN_SCHED_INTERVAL);
    Instrument next = Instrument.takeNextScheduled(interval);
    if (next == null) return Response.status(Status.NOT_FOUND).build();
    return Response.ok().entity(next).build();
  }

}
