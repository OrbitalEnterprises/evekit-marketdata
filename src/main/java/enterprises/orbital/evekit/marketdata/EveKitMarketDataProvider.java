package enterprises.orbital.evekit.marketdata;

import enterprises.orbital.base.OrbitalProperties;
import enterprises.orbital.db.ConnectionFactory;

public class EveKitMarketDataProvider {
  public static final String MARKETDATA_PU_PROP    = "enterprises.orbital.evekit.marketdata.persistence_unit";
  public static final String MARKETDATA_PU_DEFAULT = "evekit-marketdata";
  public static final String CREST_ROOT_PROP         = "enterprises.orbital.evekit.marketdata.crest_root";
  public static final String CREST_ROOT_DEFAULT      = "https://crest.eveonline.com/";

  public static ConnectionFactory getFactory() {
    return ConnectionFactory.getFactory(OrbitalProperties.getGlobalProperty(MARKETDATA_PU_PROP, MARKETDATA_PU_DEFAULT));
  }

}
