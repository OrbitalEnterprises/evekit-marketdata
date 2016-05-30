package enterprises.orbital.evekit.marketdata.model;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.db.ConnectionFactory;

public class EveKitMarketDataProvider {
  public static final String PROP_MARKETDATA_PU = "enterprises.orbital.evekit.marketdata.persistence_unit";
  public static final String DFLT_MARKETDATA_PU = "evekit-marketdata";

  public static ConnectionFactory getFactory() {
    return ConnectionFactory.getFactory(OrbitalProperties.getGlobalProperty(PROP_MARKETDATA_PU, DFLT_MARKETDATA_PU));
  }

}
