package org.tikv.common.util;

import com.google.gson.Gson;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiKVPDHttpClient {
  private static final Logger LOG = LoggerFactory.getLogger(TiKVPDHttpClient.class);
  public static final byte LEFT_BRACE = (byte) '{';
  public static final byte RIGHT_BRACE = (byte) '}';

  public static final String GET_REGIONS_URL_PATTERN = "%s/pd/api/v1/regions";
  public static final String SCHEDULER_URL_PARRERN = "%s/pd/api/v1/config/schedule";

  public static final String SCHEDULER_ENABLE_TIKV_SPLIT_REGION = "enable-tikv-split-region";

  private volatile List<String> pdAddrs;

  private final ConcurrentHashMap<String, CloseableHttpClient> urlToClientMap =
      new ConcurrentHashMap<>();

  public TiKVPDHttpClient(String pd) {
    // Parse pd address
    if (pd == null || pd.isEmpty()) {
      throw new IllegalArgumentException("PD can not be null");
    }
    String[] pds = pd.split(",");
    pdAddrs = new ArrayList<>(pds.length);
    for (String pdAddr : pds) {
      pdAddrs.add(pdAddr);
    }
  }

  public TiKVPDHttpClient(List<URI> pdURIs) {
    // Parse pd address
    if (pdURIs == null || pdURIs.isEmpty()) {
      throw new IllegalArgumentException("PD can not be null");
    }
    pdAddrs = new ArrayList<>(pdURIs.size());
    for (URI pdURI : pdURIs) {
      pdAddrs.add(pdURI.toString());
    }
  }

  public boolean isRegionSplitEnable() throws IOException {
    IOException ioe = null;
    for (String pd : pdAddrs) { // masters is not empty
      try {
        return isRegionSplitEnable(pd);
      } catch (IOException e) {
        String log =
            String.format(
                "Get parameter %s from pd %s failed ", SCHEDULER_ENABLE_TIKV_SPLIT_REGION, pd);
        LOG.error(log, e);
        ioe = e;
      } catch (Throwable te) {
        String log =
            String.format(
                "Get parameter %s from pd %s failed ", SCHEDULER_ENABLE_TIKV_SPLIT_REGION, pd);
        LOG.error(log, te);
        ioe = new IOException(te);
      }
    }
    throw ioe;
  }

  public void regionSplitEnable(boolean enable) throws IOException {
    IOException ioe = null;
    for (String pd : pdAddrs) { // masters is not empty
      try {
        regionSplitEnable(pd, enable);
        return;
      } catch (IOException e) {
        String log =
            String.format(
                "Set parameter %s to pd %s failed ", SCHEDULER_ENABLE_TIKV_SPLIT_REGION, pd);
        LOG.error(log, e);
        ioe = e;
      } catch (Throwable te) {
        String log =
            String.format(
                "Set parameter %s to pd %s failed ", SCHEDULER_ENABLE_TIKV_SPLIT_REGION, pd);
        LOG.error(log, te);
        ioe = new IOException(te);
      }
    }

    throw ioe;
  }

  public String getAllRegionsJson() throws IOException {
    IOException ioe = null;
    for (String pd : pdAddrs) { // masters is not empty
      try {
        return getAllRegionsJson(pd);
      } catch (IOException e) {
        String log = String.format("Get regions from pd %s failed ", pd);
        LOG.error(log, e);
        ioe = e;
      } catch (Throwable te) {
        String log = String.format("Get regions from pd %s failed ", pd);
        LOG.error(log, te);
        ioe = new IOException(te);
      }
    }

    throw ioe;
  }

  private CloseableHttpClient getClient(String url) {
    CloseableHttpClient httpClient = urlToClientMap.get(url);
    if (httpClient == null) {
      synchronized (this) {
        httpClient = urlToClientMap.get(url);
        if (httpClient == null) {
          httpClient = HttpClients.createMinimal(new BasicHttpClientConnectionManager());
          urlToClientMap.put(url, httpClient);
        }
      }
    }
    return httpClient;
  }

  private HttpResponse get(String master, String url, Map<String, String> param)
      throws IOException {
    try {
      URIBuilder builder = new URIBuilder(url);
      if (null != param) {
        for (Entry<String, String> entry : param.entrySet()) {
          builder.addParameter(entry.getKey(), entry.getValue());
        }
      }

      HttpGet get = new HttpGet(builder.build());
      CloseableHttpClient httpClient = getClient(master);
      synchronized (httpClient) {
        return httpClient.execute(get);
      }

    } catch (URISyntaxException e) {
      throw new RuntimeException("unexpected exception", e);
    }
  }

  private HttpResponse post(String master, String url, Map<String, String> param)
      throws IOException {
    try {
      URIBuilder builder = new URIBuilder(url);
      HttpPost post = new HttpPost(builder.build());
      Gson gson = new Gson();
      String jsonParam = gson.toJson(param);
      StringEntity entity = new StringEntity(jsonParam, ContentType.APPLICATION_JSON);
      post.setEntity(entity);

      CloseableHttpClient httpClient = getClient(master);
      synchronized (httpClient) {
        return httpClient.execute(post);
      }

    } catch (URISyntaxException e) {
      throw new RuntimeException("unexpected exception", e);
    }
  }

  private void adjustMasters(int index) {
    if (index == 0 || index >= pdAddrs.size()) {
      return;
    }

    int pdNum = pdAddrs.size();
    List<String> newPds = new ArrayList<>(pdNum);
    for (int i = index; i < pdNum; i++) {
      newPds.add(pdAddrs.get(i));
    }

    for (int i = 0; i < index; i++) {
      newPds.add(pdAddrs.get(i));
    }

    pdAddrs = newPds;
  }

  private boolean isRegionSplitEnable(String pd) throws IOException {
    Map<String, Object> params = getSchedulerConfig(pd);
    return getBoolean(params, SCHEDULER_ENABLE_TIKV_SPLIT_REGION, false);
  }

  private void regionSplitEnable(String pd, boolean enable) throws IOException {
    String response = httpRegionSplitEnable(pd, enable);
    System.out.println("Response is " + response);
  }

  private Map<String, Object> getSchedulerConfig(String pd) throws IOException {
    String url = String.format(SCHEDULER_URL_PARRERN, pd);
    HttpResponse response = get(pd, url, new HashMap<>());
    if (response.getStatusLine().getStatusCode() == 200) {
      String json = getJsonFromResponse(response);
      Gson gson = new Gson();
      return gson.fromJson(json, HashMap.class);
    } else {
      // Http error
      throw new IOException(
          String.format(
              "query model error, url=%s, httpReturn=%s, details=%s",
              url, response.getStatusLine(), getResponseStr(response)));
    }
  }

  private String getJsonFromResponse(HttpResponse response) throws IOException {
    // Read all response data to a byte array stream
    byte[] buf = new byte[1024];
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    InputStream stream = response.getEntity().getContent();
    int readLen = stream.read(buf);
    while (readLen > 0) {
      out.write(buf, 0, readLen);
      readLen = stream.read(buf);
    }

    // Generate json string
    String json = out.toString("UTF-8");

    if (json.charAt(0) == '"' && json.charAt(json.length() - 1) == '"') {
      json = json.substring(1, json.length() - 1);
    }
    return json;
  }

  private String getAllRegionsJson(String pd) throws IOException {
    String url = String.format(GET_REGIONS_URL_PATTERN, pd);
    HttpResponse response = get(pd, url, new HashMap<>());
    if (response.getStatusLine().getStatusCode() == 200) {
      return getJsonFromResponse(response);
    } else {
      // Http error
      throw new IOException(
          String.format(
              "query model error, url=%s, httpReturn=%s, details=%s",
              url, response.getStatusLine(), getResponseStr(response)));
    }
  }

  private String getSchedulerConfigJson(String pd) throws IOException {
    String url = String.format(SCHEDULER_URL_PARRERN, pd);
    HttpResponse response = get(pd, url, new HashMap<>());
    if (response.getStatusLine().getStatusCode() == 200) {
      return getJsonFromResponse(response);
    } else {
      // Http error
      throw new IOException(
          String.format(
              "query model error, url=%s, httpReturn=%s, details=%s",
              url, response.getStatusLine(), getResponseStr(response)));
    }
  }

  private String httpRegionSplitEnable(String pd, boolean enable) throws IOException {
    String url = String.format(SCHEDULER_URL_PARRERN, pd);
    HashMap<String, String> params = new HashMap<>();
    params.put(SCHEDULER_ENABLE_TIKV_SPLIT_REGION, String.valueOf(enable));
    HttpResponse response = post(pd, url, params);
    if (response.getStatusLine().getStatusCode() == 200) {
      return getJsonFromResponse(response);
    } else {
      // Http error
      throw new IOException(
          String.format(
              "query model error, url=%s, httpReturn=%s, details=%s",
              url, response.getStatusLine(), getResponseStr(response)));
    }
  }

  private String getResponseStr(HttpResponse response) throws IOException {
    // Read all response data to a byte array stream
    byte[] buf = new byte[1024];
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    HttpEntity entity = response.getEntity();
    if (entity != null) {
      InputStream stream = entity.getContent();
      if (stream != null) {
        int readLen = stream.read(buf);
        while (readLen > 0) {
          out.write(buf, 0, readLen);
          readLen = stream.read(buf);
        }
      }
    }

    // Generate json string
    String str = out.toString("UTF-8");
    return str;
  }

  public void close() {
    for (Entry<String, CloseableHttpClient> pdToClient : urlToClientMap.entrySet()) {
      LOG.info("Close http connection to " + pdToClient.getKey());
      try {
        pdToClient.getValue().close();
      } catch (IOException e) {
        LOG.warn("Close http connection to " + pdToClient.getKey() + " falied ", e);
      }
    }
  }

  public static void main(String[] args) {
    String pd = args[0];
    TiKVPDHttpClient pdHttpClient = new TiKVPDHttpClient(pd);
    try {
      boolean splitEnable = pdHttpClient.isRegionSplitEnable();
      LOG.info("split enable is " + splitEnable);
      pdHttpClient.regionSplitEnable(!splitEnable);
      boolean newSplitEnable = pdHttpClient.isRegionSplitEnable();
      LOG.info("new split enable is " + newSplitEnable);
    } catch (IOException e) {
      LOG.error("Get regions failed ", e);
    }
  }

  public static boolean getBoolean(Map<String, Object> params, String key, boolean defaultValue) {
    Object value = params.get(key);
    if (value == null) {
      return defaultValue;
    }

    if (value instanceof String) {
      return "true".equalsIgnoreCase(((String) value));
    } else if (value instanceof Boolean) {
      return (Boolean) value;
    } else {
      throw new UnsupportedOperationException("Only support String/Boolean for boolean value");
    }
  }
}
