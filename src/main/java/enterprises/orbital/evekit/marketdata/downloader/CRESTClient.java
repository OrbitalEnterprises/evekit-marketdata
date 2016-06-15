package enterprises.orbital.evekit.marketdata.downloader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

/**
 * Simple CREST client. Doesn't support authentication yet. Instantiate this client with a root URL. Use the "fields" and "down"/"up" methods to crawl the
 * result object.
 * 
 */
public class CRESTClient {
  protected static String           agent          = null;
  protected static boolean          keepHeaders    = false;
  protected static int              connectTimeout = -1;
  private URL                       root;
  private CRESTClient               parent         = null;
  private CRESTClient               next           = null;
  private CRESTClient               prev           = null;
  private JsonObject                data;
  private Map<String, List<String>> headers        = null;
  // NOTE: synchronization not important since data is essentially immutable. At worst, we may be wasteful and have duplicate clients for the same down link.
  private Map<URL, CRESTClient>     downMap        = new HashMap<URL, CRESTClient>();

  public static void setAgent(
                              String a) {
    CRESTClient.agent = a;
  }

  public static void setKeepHeaders(
                                    boolean k) {
    CRESTClient.keepHeaders = k;
  }

  public static void setConnectTimeout(
                                       int connectTimeout) {
    CRESTClient.connectTimeout = connectTimeout;
  }

  public CRESTClient(URL root) throws IOException {
    this.root = root;
    populate();
  }

  private CRESTClient(URL root, CRESTClient parent) throws IOException {
    this.root = root;
    this.parent = parent;
    populate();
  }

  private void populate() throws IOException {
    HttpURLConnection conn = (HttpURLConnection) root.openConnection();
    if (agent != null) conn.setRequestProperty("User-Agent", agent);
    if (connectTimeout > 0) conn.setConnectTimeout(connectTimeout);
    conn.setUseCaches(true);
    JsonReader reader = Json.createReader(new InputStreamReader(root.openStream()));
    if (keepHeaders) headers = conn.getHeaderFields();
    data = reader.readObject();
    reader.close();
  }

  public CRESTClient root() {
    CRESTClient r = this;
    while (r.hasParent()) {
      r = r.up();
    }
    return r;
  }

  public boolean hasParent() {
    return parent != null;
  }

  public CRESTClient up() {
    return parent != null ? parent : this;
  }

  public CRESTClient down(
                          String url)
    throws IOException {
    URL loc = new URL(url);
    return down(loc);
  }

  public CRESTClient down(
                          URL url)
    throws IOException {
    if (downMap.containsKey(url)) return downMap.get(url);
    CRESTClient down = new CRESTClient(url, this);
    downMap.put(url, down);
    return down;
  }

  public CRESTClient first() {
    CRESTClient r = this;
    while (r.hasPrev()) {
      r = r.prev();
    }
    return r;
  }

  public boolean hasNext() {
    return next != null || data.containsKey("next");
  }

  public boolean hasPrev() {
    return prev != null;
  }

  public CRESTClient prev() {
    return prev != null ? prev : this;
  }

  public CRESTClient next() throws IOException {
    if (!hasNext()) return this;
    if (next != null) return next;
    URL loc = new URL(data.getJsonObject("next").getString("href"));
    next = new CRESTClient(loc, parent);
    next.prev = this;
    return next;
  }

  public JsonObject getData() {
    return data;
  }

  public Map<String, List<String>> getHeaders() {
    return headers;
  }

  public static void main(
                          String[] argv) {

  }

}
