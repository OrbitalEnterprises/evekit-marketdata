package enterprises.orbital.evekit.marketdata;

import java.math.BigDecimal;

/**
 * Representation of a market order (either buy or sell).
 */
public class Order {
  private int        regionID;
  private int        typeID;
  private long       orderID;
  private boolean    buy;
  private long       issued;
  private BigDecimal price;
  private int        volumeEntered;
  private int        minVolume;
  private int        volume;
  private String     orderRange;
  private long       locationID;
  private int        duration;

  protected Order() {}

  public Order(int regionID, int typeID, long orderID, boolean buy, long issued, BigDecimal price, int volumeEntered, int minVolume, int volume, String range,
               long locationID, int duration) {
    super();
    this.regionID = regionID;
    this.typeID = typeID;
    this.orderID = orderID;
    this.buy = buy;
    this.issued = issued;
    this.price = price;
    this.volumeEntered = volumeEntered;
    this.minVolume = minVolume;
    this.volume = volume;
    this.orderRange = range;
    this.locationID = locationID;
    this.duration = duration;
  }

  public int getRegionID() {
    return regionID;
  }

  public void setRegionID(
                          int regionID) {
    this.regionID = regionID;
  }

  public int getTypeID() {
    return typeID;
  }

  public void setTypeID(
                        int typeID) {
    this.typeID = typeID;
  }

  public long getOrderID() {
    return orderID;
  }

  public void setOrderID(
                         long orderID) {
    this.orderID = orderID;
  }

  public boolean isBuy() {
    return buy;
  }

  public void setBuy(
                     boolean buy) {
    this.buy = buy;
  }

  public long getIssued() {
    return issued;
  }

  public void setIssued(
                        long issued) {
    this.issued = issued;
  }

  public BigDecimal getPrice() {
    return price;
  }

  public void setPrice(
                       BigDecimal price) {
    this.price = price;
  }

  public int getVolumeEntered() {
    return volumeEntered;
  }

  public void setVolumeEntered(
                               int volumeEntered) {
    this.volumeEntered = volumeEntered;
  }

  public int getMinVolume() {
    return minVolume;
  }

  public void setMinVolume(
                           int minVolume) {
    this.minVolume = minVolume;
  }

  public int getVolume() {
    return volume;
  }

  public void setVolume(
                        int volume) {
    this.volume = volume;
  }

  public String getOrderRange() {
    return orderRange;
  }

  public void setOrderRange(
                            String range) {
    this.orderRange = range;
  }

  public long getLocationID() {
    return locationID;
  }

  public void setLocationID(
                            long locationID) {
    this.locationID = locationID;
  }

  public int getDuration() {
    return duration;
  }

  public void setDuration(
                          int duration) {
    this.duration = duration;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (buy ? 1231 : 1237);
    result = prime * result + duration;
    result = prime * result + (int) (issued ^ (issued >>> 32));
    result = prime * result + (int) (locationID ^ (locationID >>> 32));
    result = prime * result + minVolume;
    result = prime * result + (int) (orderID ^ (orderID >>> 32));
    result = prime * result + ((orderRange == null) ? 0 : orderRange.hashCode());
    result = prime * result + ((price == null) ? 0 : price.hashCode());
    result = prime * result + regionID;
    result = prime * result + typeID;
    result = prime * result + volume;
    result = prime * result + volumeEntered;
    return result;
  }

  @Override
  public boolean equals(
                        Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    Order other = (Order) obj;
    if (buy != other.buy) return false;
    if (duration != other.duration) return false;
    if (issued != other.issued) return false;
    if (locationID != other.locationID) return false;
    if (minVolume != other.minVolume) return false;
    if (orderID != other.orderID) return false;
    if (orderRange == null) {
      if (other.orderRange != null) return false;
    } else if (!orderRange.equals(other.orderRange)) return false;
    if (price == null) {
      if (other.price != null) return false;
    } else if (!price.equals(other.price)) return false;
    if (regionID != other.regionID) return false;
    if (typeID != other.typeID) return false;
    if (volume != other.volume) return false;
    if (volumeEntered != other.volumeEntered) return false;
    return true;
  }

  @Override
  public String toString() {
    return "Order [regionID=" + regionID + ", typeID=" + typeID + ", orderID=" + orderID + ", buy=" + buy + ", issued=" + issued + ", price=" + price
        + ", volumeEntered=" + volumeEntered + ", minVolume=" + minVolume + ", volume=" + volume + ", orderRange=" + orderRange + ", locationID=" + locationID
        + ", duration=" + duration + "]";
  }

}
