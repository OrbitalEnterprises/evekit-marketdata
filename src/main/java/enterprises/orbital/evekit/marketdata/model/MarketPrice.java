package enterprises.orbital.evekit.marketdata.model;

import java.math.BigDecimal;

/**
 * Representation of market price information for a type.
 */
public class MarketPrice {
  private int        typeID;
  private BigDecimal adjustedPrice;
  private BigDecimal averagePrice;
  private long       date;

  protected MarketPrice() {}

  public MarketPrice(int typeID, BigDecimal adjustedPrice, BigDecimal averagePrice, long date) {
    super();
    this.typeID = typeID;
    this.adjustedPrice = adjustedPrice;
    this.averagePrice = averagePrice;
    this.date = date;
  }

  public int getTypeID() {
    return typeID;
  }

  public void setTypeID(
                        int typeID) {
    this.typeID = typeID;
  }

  public BigDecimal getAdjustedPrice() {
    return adjustedPrice;
  }

  public void setAdjustedPrice(
                               BigDecimal adjustedPrice) {
    this.adjustedPrice = adjustedPrice;
  }

  public BigDecimal getAveragePrice() {
    return averagePrice;
  }

  public void setAveragePrice(
                              BigDecimal averagePrice) {
    this.averagePrice = averagePrice;
  }

  public long getDate() {
    return date;
  }

  public void setDate(
                      long date) {
    this.date = date;
  }

}
