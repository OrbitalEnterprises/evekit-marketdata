package enterprises.orbital.evekit.marketdata;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.NoResultException;
import javax.persistence.Table;
import javax.persistence.TypedQuery;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.db.ConnectionFactory.RunInTransaction;

/**
 * Representation of a market region
 */
@Entity
@Table(
    name = "ekmd_region",
    indexes = {
        @Index(
            name = "regionIndex",
            columnList = "regionID",
            unique = false),
        @Index(
            name = "activeIndex",
            columnList = "active",
            unique = false),
        @Index(
            name = "lastUpdateIndex",
            columnList = "lastUpdate",
            unique = false),
        @Index(
            name = "scheduledIndex",
            columnList = "scheduled",
            unique = false),
        @Index(
            name = "scheduleTimeIndex",
            columnList = "scheduleTime",
            unique = false),
    })
@NamedQueries({
    @NamedQuery(
        name = "Region.getRegion",
        query = "SELECT i FROM Region i WHERE i.regionID = :rid"),
    @NamedQuery(
        name = "Region.getNextScheduled",
        query = "SELECT i FROM Region i WHERE i.active = true AND i.scheduled = false ORDER BY i.lastUpdate asc"),
    @NamedQuery(
        name = "Region.getActiveRegionIDs",
        query = "SELECT i.regionID FROM Region i WHERE i.active = true"),
    @NamedQuery(
        name = "Region.getScheduledDelayed",
        query = "SELECT i FROM Region i WHERE i.active = true AND i.scheduled = true AND i.scheduleTime < :when"),
})
public class Region {
  @Id
  private int     regionID;
  private boolean active;
  private long    lastUpdate;
  private boolean scheduled;
  private long    scheduleTime;

  protected Region() {}

  public Region(int regionID, boolean active, long lastUpdate) {
    super();
    this.regionID = regionID;
    this.active = active;
    this.lastUpdate = lastUpdate;
    this.scheduled = false;
    this.scheduleTime = 0L;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(
                        boolean active) {
    this.active = active;
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  public void setLastUpdate(
                            long lastUpdate) {
    this.lastUpdate = lastUpdate;
  }

  public int getRegionID() {
    return regionID;
  }

  public boolean isScheduled() {
    return scheduled;
  }

  public void setScheduled(
                           boolean scheduled) {
    this.scheduled = scheduled;
  }

  public long getScheduleTime() {
    return scheduleTime;
  }

  public void setScheduleTime(
                              long scheduleTime) {
    this.scheduleTime = scheduleTime;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (active ? 1231 : 1237);
    result = prime * result + (int) (lastUpdate ^ (lastUpdate >>> 32));
    result = prime * result + regionID;
    result = prime * result + (int) (scheduleTime ^ (scheduleTime >>> 32));
    result = prime * result + (scheduled ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(
                        Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    Region other = (Region) obj;
    if (active != other.active) return false;
    if (lastUpdate != other.lastUpdate) return false;
    if (regionID != other.regionID) return false;
    if (scheduleTime != other.scheduleTime) return false;
    if (scheduled != other.scheduled) return false;
    return true;
  }

  @Override
  public String toString() {
    return "Region [regionID=" + regionID + ", active=" + active + ", lastUpdate=" + lastUpdate + ", scheduled=" + scheduled + ", scheduleTime=" + scheduleTime
        + "]";
  }

  public static Region get(
                           final int rid)
    throws IOException, ExecutionException {
    return EveKitMarketDataProvider.getFactory().runTransaction(new RunInTransaction<Region>() {
      @Override
      public Region run() throws Exception {
        TypedQuery<Region> getter = EveKitMarketDataProvider.getFactory().getEntityManager().createNamedQuery("Region.getRegion", Region.class);
        getter.setParameter("rid", rid);
        try {
          return getter.getSingleResult();
        } catch (NoResultException e) {
          return null;
        }
      }
    });
  }

  public static Region update(
                              final Region data)
    throws IOException, ExecutionException {
    return EveKitMarketDataProvider.getFactory().runTransaction(new RunInTransaction<Region>() {
      @Override
      public Region run() throws Exception {
        return EveKitMarketDataProvider.getFactory().getEntityManager().merge(data);
      }
    });
  }

  public static Region getNextScheduled() throws IOException, ExecutionException {
    return EveKitMarketDataProvider.getFactory().runTransaction(new RunInTransaction<Region>() {
      @Override
      public Region run() throws Exception {
        TypedQuery<Region> getter = EveKitMarketDataProvider.getFactory().getEntityManager().createNamedQuery("Region.getNextScheduled", Region.class);
        getter.setMaxResults(1);
        try {
          return getter.getSingleResult();
        } catch (NoResultException e) {
          return null;
        }
      }
    });
  }

  public static Region takeNextScheduled(
                                         final long minSchedInterval)
    throws IOException, ExecutionException {
    return EveKitMarketDataProvider.getFactory().runTransaction(new RunInTransaction<Region>() {
      @Override
      public Region run() throws Exception {
        Region next = getNextScheduled();
        if (next == null) return null;
        if (next.getLastUpdate() + minSchedInterval > OrbitalProperties.getCurrentTime())
          // Not enough time has elapsed since the last time this region was scheduled
          return null;
        next.setScheduled(true);
        next.setScheduleTime(OrbitalProperties.getCurrentTime());
        next = update(next);
        return next;
      }
    });
  }

  public static List<Integer> getActiveRegionIDs() throws IOException, ExecutionException {
    return EveKitMarketDataProvider.getFactory().runTransaction(new RunInTransaction<List<Integer>>() {
      @Override
      public List<Integer> run() throws Exception {
        TypedQuery<Integer> getter = EveKitMarketDataProvider.getFactory().getEntityManager().createNamedQuery("Region.getActiveRegionIDs", Integer.class);
        return getter.getResultList();
      }
    });
  }

  public static List<Region> getDelayed(
                                        final long threshold)
    throws IOException, ExecutionException {
    return EveKitMarketDataProvider.getFactory().runTransaction(new RunInTransaction<List<Region>>() {
      @Override
      public List<Region> run() throws Exception {
        TypedQuery<Region> getter = EveKitMarketDataProvider.getFactory().getEntityManager().createNamedQuery("Region.getScheduledDelayed", Region.class);
        getter.setParameter("when", threshold);
        return getter.getResultList();
      }
    });
  }

}
