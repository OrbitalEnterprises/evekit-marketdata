package enterprises.orbital.evekit.marketdata;

import java.math.BigDecimal;

/**
 * Representation of market price history for a type and region.
 */
public class MarketHistory {
  private int        typeID;
  private int        regionID;
  private int        orderCount;
  private BigDecimal lowPrice;
  private BigDecimal highPrice;
  private BigDecimal avgPrice;
  private long       volume;
  private long       date;

  protected MarketHistory() {}

  public MarketHistory(int typeID, int regionID, int orderCount, BigDecimal lowPrice, BigDecimal highPrice, BigDecimal avgPrice, long volume, long date) {
    super();
    this.typeID = typeID;
    this.regionID = regionID;
    this.orderCount = orderCount;
    this.lowPrice = lowPrice;
    this.highPrice = highPrice;
    this.avgPrice = avgPrice;
    this.volume = volume;
    this.date = date;
  }

  public int getTypeID() {
    return typeID;
  }

  public void setTypeID(
                        int typeID) {
    this.typeID = typeID;
  }

  public int getRegionID() {
    return regionID;
  }

  public void setRegionID(
                          int regionID) {
    this.regionID = regionID;
  }

  public int getOrderCount() {
    return orderCount;
  }

  public void setOrderCount(
                            int orderCount) {
    this.orderCount = orderCount;
  }

  public BigDecimal getLowPrice() {
    return lowPrice;
  }

  public void setLowPrice(
                          BigDecimal lowPrice) {
    this.lowPrice = lowPrice;
  }

  public BigDecimal getHighPrice() {
    return highPrice;
  }

  public void setHighPrice(
                           BigDecimal highPrice) {
    this.highPrice = highPrice;
  }

  public BigDecimal getAvgPrice() {
    return avgPrice;
  }

  public void setAvgPrice(
                          BigDecimal avgPrice) {
    this.avgPrice = avgPrice;
  }

  public long getVolume() {
    return volume;
  }

  public void setVolume(
                        long volume) {
    this.volume = volume;
  }

  public long getDate() {
    return date;
  }

  public void setDate(
                      long date) {
    this.date = date;
  }

}
