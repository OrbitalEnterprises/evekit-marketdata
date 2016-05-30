package enterprises.orbital.evekit.marketdata.model;

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
 * Representation of a tradeable market instrument (aka market type)
 */
@Entity
@Table(
    name = "ekmd_instrument",
    indexes = {
        @Index(
            name = "typeIndex",
            columnList = "typeID",
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
        name = "Instrument.getInstrument",
        query = "SELECT i FROM Instrument i WHERE i.typeID = :tid"),
    @NamedQuery(
        name = "Instrument.getNextScheduled",
        query = "SELECT i FROM Instrument i WHERE i.active = true AND i.scheduled = false ORDER BY i.lastUpdate asc"),
    @NamedQuery(
        name = "Instrument.getActiveInstrumentIDs",
        query = "SELECT i.typeID FROM Instrument i WHERE i.active = true"),
    @NamedQuery(
        name = "Instrument.getScheduledDelayed",
        query = "SELECT i FROM Instrument i WHERE i.active = true AND i.scheduled = true AND i.scheduleTime < :when"),
})
public class Instrument {
  @Id
  private int     typeID;
  private boolean active;
  private long    lastUpdate;
  private boolean scheduled;
  private long    scheduleTime;

  protected Instrument() {}

  public Instrument(int typeID, boolean active, long lastUpdate) {
    super();
    this.typeID = typeID;
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

  public int getTypeID() {
    return typeID;
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
    result = prime * result + (int) (scheduleTime ^ (scheduleTime >>> 32));
    result = prime * result + (scheduled ? 1231 : 1237);
    result = prime * result + typeID;
    return result;
  }

  @Override
  public boolean equals(
                        Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    Instrument other = (Instrument) obj;
    if (active != other.active) return false;
    if (lastUpdate != other.lastUpdate) return false;
    if (scheduleTime != other.scheduleTime) return false;
    if (scheduled != other.scheduled) return false;
    if (typeID != other.typeID) return false;
    return true;
  }

  @Override
  public String toString() {
    return "Instrument [typeID=" + typeID + ", active=" + active + ", lastUpdate=" + lastUpdate + ", scheduled=" + scheduled + ", scheduleTime=" + scheduleTime
        + "]";
  }

  public static Instrument get(
                               final int tid)
    throws IOException, ExecutionException {
    return EveKitMarketDataProvider.getFactory().runTransaction(new RunInTransaction<Instrument>() {
      @Override
      public Instrument run() throws Exception {
        TypedQuery<Instrument> getter = EveKitMarketDataProvider.getFactory().getEntityManager().createNamedQuery("Instrument.getInstrument", Instrument.class);
        getter.setParameter("tid", tid);
        try {
          return getter.getSingleResult();
        } catch (NoResultException e) {
          return null;
        }
      }
    });
  }

  public static Instrument update(
                                  final Instrument data)
    throws IOException, ExecutionException {
    return EveKitMarketDataProvider.getFactory().runTransaction(new RunInTransaction<Instrument>() {
      @Override
      public Instrument run() throws Exception {
        return EveKitMarketDataProvider.getFactory().getEntityManager().merge(data);
      }
    });
  }

  public static Instrument getNextScheduled() throws IOException, ExecutionException {
    return EveKitMarketDataProvider.getFactory().runTransaction(new RunInTransaction<Instrument>() {
      @Override
      public Instrument run() throws Exception {
        TypedQuery<Instrument> getter = EveKitMarketDataProvider.getFactory().getEntityManager().createNamedQuery("Instrument.getNextScheduled",
                                                                                                                  Instrument.class);
        getter.setMaxResults(1);
        try {
          return getter.getSingleResult();
        } catch (NoResultException e) {
          return null;
        }
      }
    });
  }

  public static Instrument takeNextScheduled(
                                             final long minSchedInterval)
    throws IOException, ExecutionException {
    return EveKitMarketDataProvider.getFactory().runTransaction(new RunInTransaction<Instrument>() {
      @Override
      public Instrument run() throws Exception {
        Instrument next = getNextScheduled();
        if (next == null) return null;
        if (next.getLastUpdate() + minSchedInterval > OrbitalProperties.getCurrentTime())
          // Not enough time has elapsed since the last time this instrument was scheduled
          return null;
        next.setScheduled(true);
        next.setScheduleTime(OrbitalProperties.getCurrentTime());
        next = update(next);
        return next;
      }
    });
  }

  public static List<Integer> getActiveTypeIDs() throws IOException, ExecutionException {
    return EveKitMarketDataProvider.getFactory().runTransaction(new RunInTransaction<List<Integer>>() {
      @Override
      public List<Integer> run() throws Exception {
        TypedQuery<Integer> getter = EveKitMarketDataProvider.getFactory().getEntityManager().createNamedQuery("Instrument.getActiveInstrumentIDs",
                                                                                                               Integer.class);
        return getter.getResultList();
      }
    });
  }

  public static List<Instrument> getDelayed(
                                            final long threshold)
    throws IOException, ExecutionException {
    return EveKitMarketDataProvider.getFactory().runTransaction(new RunInTransaction<List<Instrument>>() {
      @Override
      public List<Instrument> run() throws Exception {
        TypedQuery<Instrument> getter = EveKitMarketDataProvider.getFactory().getEntityManager().createNamedQuery("Instrument.getScheduledDelayed",
                                                                                                                  Instrument.class);
        getter.setParameter("when", threshold);
        return getter.getResultList();
      }
    });
  }


}
