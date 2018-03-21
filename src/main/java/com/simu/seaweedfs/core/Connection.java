

package com.simu.seaweedfs.core;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.simu.seaweedfs.core.contect.LookupVolumeResult;
import com.simu.seaweedfs.core.contect.PreAllocateVolumesParams;
import com.simu.seaweedfs.core.file.Size;
import com.simu.seaweedfs.core.file.SizeUnit;
import com.simu.seaweedfs.core.http.HeaderResponse;
import com.simu.seaweedfs.core.topology.*;
import com.simu.seaweedfs.util.WarningSendUtil;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.client.cache.HttpCacheStorage;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import com.simu.seaweedfs.core.contect.ForceGarbageCollectionParams;
import com.simu.seaweedfs.core.http.JsonResponse;
import com.simu.seaweedfs.core.http.StreamResponse;
import com.simu.seaweedfs.exception.SeaweedfsException;
import com.simu.seaweedfs.util.ConnectionUtil;
import com.simu.seaweedfs.util.RequestPathStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Connection for seaweed file system.
 *
 * @author ChihoSin modified by DengrongGuan
 */
class Connection {

    static final String LOOKUP_VOLUME_CACHE_ALIAS = "lookupVolumeCache";

    private static final Logger log = LoggerFactory.getLogger(Connection.class);

    private String leaderUrl;
    private long statusExpiry;
    private long volumeStatusCheckInterval;
    private long volumeSizeWarnLimit;
    private int connectionTimeout;
    private boolean connectionClose = true;
    private boolean enableFileStreamCache;
    private int fileStreamCacheEntries;
    private long fileStreamCacheSize;
    private HttpCacheStorage fileStreamCacheStorage;
    private boolean enableLookupVolumeCache;
    private long lookupVolumeCacheExpiry;
    private int lookupVolumeCacheEntries;
    private long idleConnectionExpiry;
    private SystemClusterStatus systemClusterStatus;
    private SystemTopologyStatus systemTopologyStatus;
    private PollSystemTopologyStatusThread pollSystemTopologyStatusThread;
    private PollClusterStatusThread pollClusterStatusThread;
    private ObjectMapper objectMapper = new ObjectMapper();
    private PoolingHttpClientConnectionManager clientConnectionManager;
    private IdleConnectionMonitorThread idleConnectionMonitorThread;
    private CloseableHttpClient httpClient;
    private CacheManager cacheManager = null;
    private WarningSendUtil warningSendUtil = null;

    /**
     * Constructor, build by properties.
     *
     * @param leaderUrl                Leader server url.
     * @param connectionTimeout        Http connection timeout.
     * @param statusExpiry             Server status expiry.
     * @param volumeStatusCheckInterval
     * @param volumeSizeWarnLimit
     * @param idleConnectionExpiry     Http connection idle expiry.
     * @param maxConnection            Max http connection.
     * @param maxConnectionsPreRoute   Max connections pre route.
     * @param enableLookupVolumeCache  Enable lookup volume cache.
     * @param lookupVolumeCacheExpiry  Lookup volume cache expiry.
     * @param lookupVolumeCacheEntries Lookup volume cache entries.
     * @param enableFileStreamCache    Enable file stream cache.
     * @param fileStreamCacheEntries   File stream cache entries.
     * @param fileStreamCacheSize      File stream cache size.
     * @param fileStreamCacheStorage   File stream cache storage.
     * @throws IOException Http connection is fail or server response within some error message.
     */
    Connection(String leaderUrl, int connectionTimeout, long statusExpiry, long volumeStatusCheckInterval, long volumeSizeWarnLimit, long idleConnectionExpiry,
               int maxConnection, int maxConnectionsPreRoute, boolean enableLookupVolumeCache,
               long lookupVolumeCacheExpiry, int lookupVolumeCacheEntries,
               boolean enableFileStreamCache, int fileStreamCacheEntries, long fileStreamCacheSize,
               HttpCacheStorage fileStreamCacheStorage,WarningSendUtil warningSendUtil)
            throws IOException {
        this.leaderUrl = leaderUrl;
        this.statusExpiry = statusExpiry;
        this.volumeSizeWarnLimit = volumeSizeWarnLimit;
        this.volumeStatusCheckInterval = volumeStatusCheckInterval;
        this.connectionTimeout = connectionTimeout;
        this.idleConnectionExpiry = idleConnectionExpiry;
        this.enableLookupVolumeCache = enableLookupVolumeCache;
        this.lookupVolumeCacheExpiry = lookupVolumeCacheExpiry;
        this.lookupVolumeCacheEntries = lookupVolumeCacheEntries;
        this.pollSystemTopologyStatusThread = new PollSystemTopologyStatusThread();
        this.pollClusterStatusThread = new PollClusterStatusThread();
        this.idleConnectionMonitorThread = new IdleConnectionMonitorThread();
        this.clientConnectionManager = new PoolingHttpClientConnectionManager();
        this.clientConnectionManager.setMaxTotal(maxConnection);
        this.clientConnectionManager.setDefaultMaxPerRoute(maxConnectionsPreRoute);
        this.enableFileStreamCache = enableFileStreamCache;
        this.fileStreamCacheEntries = fileStreamCacheEntries;
        this.fileStreamCacheSize = fileStreamCacheSize;
        this.fileStreamCacheStorage = fileStreamCacheStorage;
        this.warningSendUtil = warningSendUtil;
    }

    /**
     * Start up polls for core leader.
     */
    void startup() {
        final RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(this.connectionTimeout)
                .build();
        if (this.enableFileStreamCache) {
            if (this.fileStreamCacheStorage == null) {
                final CacheConfig cacheConfig = CacheConfig.custom()
                        .setMaxCacheEntries(this.fileStreamCacheEntries)
                        .setMaxObjectSize(this.fileStreamCacheSize)
                        .setHeuristicCachingEnabled(true)
                        .setHeuristicCoefficient(0.8f)
                        .build();
                this.httpClient = CachingHttpClients.custom()
                        .setCacheConfig(cacheConfig)
                        .setConnectionManager(this.clientConnectionManager)
                        .setDefaultRequestConfig(requestConfig)
                        .build();
            } else {
                this.httpClient = CachingHttpClients.custom()
                        .setHttpCacheStorage(this.fileStreamCacheStorage)
                        .setConnectionManager(this.clientConnectionManager)
                        .setDefaultRequestConfig(requestConfig)
                        .build();
            }
        } else {
            this.httpClient = HttpClients.custom()
                    .setConnectionManager(this.clientConnectionManager)
                    .setDefaultRequestConfig(requestConfig)
                    .build();
        }
        initCache();
        this.pollClusterStatusThread.updateSystemStatus();
        try {
            systemTopologyStatus = fetchSystemTopologyStatus(leaderUrl);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.pollSystemTopologyStatusThread.start();
        this.pollClusterStatusThread.start();
        this.idleConnectionMonitorThread.start();
        log.info("seaweedfs master server connection is startup");
    }

    /**
     * Shutdown polls for core leader.
     */
    void stop() {
        closeCache();
        this.pollSystemTopologyStatusThread.shutdown();
        this.pollClusterStatusThread.shutdown();
        this.idleConnectionMonitorThread.shutdown();
        log.info("seaweedfs master server connection is shutdown");
    }

    /**
     * Get core server cluster status.
     *
     * @return Core cluster status.
     */
    SystemClusterStatus getSystemClusterStatus() {
        return systemClusterStatus;
    }

    /**
     * Get cluster topology status.
     *
     * @return Core topology status.
     */
    SystemTopologyStatus getSystemTopologyStatus() {
        return systemTopologyStatus;
    }

    /**
     * Check volume server status.
     *
     * @param volumeUrl Volume server url.
     * @return Volume server status.
     * @throws IOException Http connection is fail or server response within some error message.
     */
    @SuppressWarnings({"unused", "unchecked"})
    VolumeStatus getVolumeStatus(String volumeUrl) throws IOException {
        HttpGet request = new HttpGet(volumeUrl + RequestPathStrategy.checkVolumeStatus);
        JsonResponse jsonResponse = fetchJsonResultByRequest(request);
        VolumeStatus volumeStatus = objectMapper.readValue(
                jsonResponse.json.replace("{}", "null"), VolumeStatus.class);
        volumeStatus.setUrl(volumeUrl);
        return volumeStatus;
    }

    /**
     * Connection close flag.
     *
     * @return If result is false, that maybe core server is failover.
     */
    boolean isConnectionClose() {
        this.pollClusterStatusThread.updateSystemStatus();
        return this.connectionClose;
    }

    /**
     * Get cache manager.
     *
     * @return {@code null} if no such cache exists.
     */
    CacheManager getCacheManager() {
        return cacheManager;
    }

    /**
     * Get leader core server uri.
     *
     * @return Core server uri
     */
    String getLeaderUrl() {
        return this.leaderUrl;
    }

    /**
     * Fetch http API json result.
     *
     * @param request Http request.
     * @return Json fetch by http response.
     * @throws IOException Http connection is fail or server response within some error message.
     */
    JsonResponse fetchJsonResultByRequest(HttpRequestBase request) throws IOException {
        CloseableHttpResponse response = null;
        request.setHeader("Connection", "close");
        JsonResponse jsonResponse = null;

        try {
            response = httpClient.execute(request, HttpClientContext.create());
            HttpEntity entity = response.getEntity();
            jsonResponse = new JsonResponse(EntityUtils.toString(entity), response.getStatusLine().getStatusCode());
            EntityUtils.consume(entity);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException ignored) {
                }
            }
            request.releaseConnection();
        }

        if (jsonResponse.json.contains("\"error\":\"")) {
            Map map = objectMapper.readValue(jsonResponse.json, Map.class);
            final String errorMsg = (String) map.get("error");
            if (errorMsg != null)
                throw new SeaweedfsException(errorMsg);
        }

        return jsonResponse;
    }

    /**
     * Fetch http API status code.
     *
     * @param request Only http method head.
     * @return Status code.
     * @throws IOException Http connection is fail or server response within some error message.
     */
    int fetchStatusCodeByRequest(HttpHead request) throws IOException {
        CloseableHttpResponse response = null;
        request.setHeader("Connection", "close");
        int statusCode;
        try {
            response = httpClient.execute(request, HttpClientContext.create());
            statusCode = response.getStatusLine().getStatusCode();
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException ignored) {
                }
            }
            request.releaseConnection();
        }
        return statusCode;
    }

    /**
     * Fetch http API input stream cache.
     *
     * @param request Http request.
     * @return Stream fetch by http response.
     * @throws IOException Http connection is fail or server response within some error message.
     */
    StreamResponse fetchStreamCacheByRequest(HttpRequestBase request) throws IOException {
        CloseableHttpResponse response = null;
        request.setHeader("Connection", "close");
        StreamResponse cache;

        try {
            response = httpClient.execute(request, HttpClientContext.create());
            HttpEntity entity = response.getEntity();
            cache = new StreamResponse(entity.getContent(), response.getStatusLine().getStatusCode());
            EntityUtils.consume(entity);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException ignored) {
                }
            }
            request.releaseConnection();
        }
        return cache;
    }

    /**
     * Fetch http API hearers with status code(in array).
     *
     * @param request Only http method head.
     * @return Header fetch by http response.
     * @throws IOException Http connection is fail or server response within some error message.
     */
    HeaderResponse fetchHeaderByRequest(HttpHead request) throws IOException {
        CloseableHttpResponse response = null;
        request.setHeader("Connection", "close");
        HeaderResponse headerResponse;

        try {
            response = httpClient.execute(request, HttpClientContext.create());
            headerResponse = new HeaderResponse(response.getAllHeaders(), response.getStatusLine().getStatusCode());
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException ignored) {
                }
            }
            request.releaseConnection();
        }
        return headerResponse;
    }

    /**
     * Force garbage collection.
     *
     * @param garbageThreshold Garbage threshold.
     * @throws IOException Http connection is fail or server response within some error message.
     */
    void forceGarbageCollection(float garbageThreshold) throws IOException {
        MasterWrapper masterWrapper = new MasterWrapper(this);
        masterWrapper.forceGarbageCollection(new ForceGarbageCollectionParams(garbageThreshold));
    }

    /**
     * Force garbage collection.
     *
     * @throws IOException Http connection is fail or server response within some error message.
     */
    void forceGarbageCollection() throws IOException {
        MasterWrapper masterWrapper = new MasterWrapper(this);
        masterWrapper.forceGarbageCollection(new ForceGarbageCollectionParams());
    }

    /**
     * Pre-allocate volumes.
     *
     * @param sameRackCount       Same rack count.
     * @param diffRackCount       Different rack count.
     * @param diffDataCenterCount Different data center count.
     * @param count               Count.
     * @param dataCenter          Data center.
     * @param ttl                 Time to live.
     * @throws IOException IOException Http connection is fail or server response within some error message.
     */
    void preAllocateVolumes(int sameRackCount, int diffRackCount, int diffDataCenterCount, int count, String dataCenter,
                            String ttl) throws IOException {
        MasterWrapper masterWrapper = new MasterWrapper(this);
        masterWrapper.preAllocateVolumes(new PreAllocateVolumesParams(
                String.valueOf(diffDataCenterCount) + String.valueOf(diffRackCount) + String.valueOf(sameRackCount),
                count,
                dataCenter,
                ttl
        ));
    }

    /**
     * Fetch core server by seaweedfs Http API.
     *
     * @param masterUrl Core server url with scheme.
     * @return Cluster status.
     */
    @SuppressWarnings("unchecked")
    private SystemClusterStatus fetchSystemClusterStatus(String masterUrl) throws IOException {
//        if (!ConnectionUtil.checkUriAlive(masterUrl)){
//            throw new SeaweedfsException("seaweedfs master failed");
//        }
        MasterStatus leader;
        ArrayList<MasterStatus> peers;
        final HttpGet request = new HttpGet(masterUrl + RequestPathStrategy.checkClusterStatus);
        final JsonResponse jsonResponse = fetchJsonResultByRequest(request);
        Map map = objectMapper.readValue(jsonResponse.json, Map.class);

        if (map.get("Leader") != null) {
            leader = new MasterStatus((String) map.get("Leader"));
        } else {
            throw new SeaweedfsException("not found seaweedfs core leader");
        }

        peers = new ArrayList<>();

        if (map.get("Peers") != null) {
            List<String> rawPeerList = (List<String>) map.get("Peers");
            for (String url : rawPeerList) {
                MasterStatus peer = new MasterStatus(url);
                peers.add(peer);
            }
        }

        if (map.get("IsLeader") == null || !((Boolean) map.get("IsLeader"))) {
            peers.add(new MasterStatus(masterUrl.replace("http://", "")));
            peers.remove(leader);
        }
        leader.setActive(ConnectionUtil.checkUriAlive(leader.getUrl()));
        if (systemClusterStatus != null && systemClusterStatus.getLeader() != null && !systemClusterStatus.getLeader().isActive() && leader.isActive()) {
            log.info("seaweedfs core leader recover [" + leaderUrl + "]");
        }
        if (!leader.isActive()){
            sendWarning(leaderUrl + " master 服务不可用！");
            throw new SeaweedfsException("seaweedfs core leader is failover");
        }
        for (MasterStatus item : peers) {
            item.setActive(ConnectionUtil.checkUriAlive(item.getUrl()));
            if (!item.isActive()) {
                sendWarning(item.getUrl() + " master 服务不可用！");
            }
        }

        return new SystemClusterStatus(leader, peers);

    }

    private void sendWarning(String content){
        if (null != this.warningSendUtil){
            warningSendUtil.sendEmail(content);
            warningSendUtil.sendSMS(content);
        }
    }

    /**
     * Find leader core server from peer core server info.
     *
     * @param peers Peers core server.
     * @return If not found the leader, result is null.
     */
    private String findLeaderUriByPeers(List<MasterStatus> peers) throws IOException {
        if (peers == null || peers.size() == 0)
            return null;
        else {
            String result;
            for (MasterStatus item : peers) {
                final HttpGet request = new HttpGet(item.getUrl() + RequestPathStrategy.checkClusterStatus);
                Map responseMap;
                try {
                    final JsonResponse jsonResponse = fetchJsonResultByRequest(request);
                    responseMap = objectMapper.readValue(jsonResponse.json, Map.class);
                } catch (IOException e) {
                    continue;
                }
                if (responseMap.get("Leader") != null) {
                    result = ConnectionUtil.convertUrlWithScheme((String) responseMap.get("Leader"));

                    if (ConnectionUtil.checkUriAlive(result))
                        return result;
                }
            }
        }
        return null;
    }

    /**
     * Fetch topology by seaweedfs Http Api.
     *
     * @param masterUrl Core server url with scheme.
     * @return Topology status.
     */
    @SuppressWarnings("unchecked")
    private SystemTopologyStatus fetchSystemTopologyStatus(String masterUrl) throws IOException {
        final HttpGet request = new HttpGet(masterUrl + RequestPathStrategy.checkTopologyStatus);
        final JsonResponse jsonResponse = fetchJsonResultByRequest(request);
        Map map = objectMapper.readValue(jsonResponse.json, Map.class);

        // Fetch data center from json
        List<DataCenter> dataCenters = new ArrayList<>();
        ArrayList<Map<String, Object>> rawDcs =
                ((ArrayList<Map<String, Object>>) ((Map) (map.get("Topology"))).get("DataCenters"));
        if (rawDcs != null)
            for (Map<String, Object> rawDc : rawDcs) {
                DataCenter dc = new DataCenter();
                dc.setFree((Integer) rawDc.get("Free"));
                dc.setId((String) rawDc.get("Id"));
                dc.setMax((Integer) rawDc.get("Max"));

                List<Rack> racks = new ArrayList<>();
                ArrayList<Map<String, Object>> rawRks =
                        ((ArrayList<Map<String, Object>>) (rawDc.get("Racks")));
                if (rawRks != null)
                    for (Map<String, Object> rawRk : rawRks) {
                        Rack rk = new Rack();
                        rk.setMax((Integer) rawRk.get("Max"));
                        rk.setId((String) rawRk.get("Id"));
                        rk.setFree((Integer) rawRk.get("Free"));

                        List<DataNode> dataNodes = new ArrayList<>();
                        ArrayList<Map<String, Object>> rawDns =
                                ((ArrayList<Map<String, Object>>) (rawRk.get("DataNodes")));

                        if (rawDns != null)
                            for (Map<String, Object> rawDn : rawDns) {
                                DataNode dn = new DataNode();
                                dn.setFree((Integer) rawDn.get("Free"));
                                dn.setMax((Integer) rawDn.get("Max"));
                                dn.setVolumes((Integer) rawDn.get("Volumes"));
                                dn.setUrl((String) rawDn.get("Url"));
                                dn.setPublicUrl((String) rawDn.get("PublicUrl"));
                                try {
                                    HttpGet httpRequest = new HttpGet(dn.getPublicUrl() + RequestPathStrategy.checkVolumeDisk);
                                    JsonResponse response = fetchJsonResultByRequest(httpRequest);
                                    Map map1 = objectMapper.readValue(response.json, Map.class);
                                    dn.setActive(true);
                                    ArrayList<Map> diskStatuses = (ArrayList<Map>) map1.get("DiskStatuses");
                                    dn.setDir((String) diskStatuses.get(0).get("Dir"));
                                    long allSize = (Long)diskStatuses.get(0).get("All");
                                    long freeSize = (Long)diskStatuses.get(0).get("Free");
                                    if (freeSize <= volumeSizeWarnLimit){
                                        //TODO 容量告警
                                    }
                                    dn.setMaxSize(new Size(allSize, SizeUnit.B).human());
                                    dn.setFreeSize(new Size(freeSize, SizeUnit.B).human());
                                } catch (Exception e) {
                                    log.error("check volume failed:",e);
                                    dn.setActive(false);
                                }
                                dataNodes.add(dn);
                            }
                        rk.setDataNodes(dataNodes);
                        racks.add(rk);
                    }
                dc.setRacks(racks);
                dataCenters.add(dc);
            }

        // Fetch data layout
        ArrayList<Layout> layouts = new ArrayList<>();
        ArrayList<Map<String, Object>> rawLos =
                ((ArrayList<Map<String, Object>>) ((Map) (map.get("Topology"))).get("layouts"));
        if (rawLos != null)
            for (Map<String, Object> rawLo : rawLos) {
                Layout layout = new Layout();
                if (rawLo.get("collection") != null || !((String) rawLo.get("collection")).isEmpty())
                    layout.setCollection((String) rawLo.get("collection"));
                if (rawLo.get("replication") != null || !((String) rawLo.get("replication")).isEmpty())
                    layout.setReplication((String) rawLo.get("replication"));
                if (rawLo.get("ttl") != null || !((String) rawLo.get("ttl")).isEmpty())
                    layout.setTtl((String) rawLo.get("ttl"));
                if (rawLo.get("writables") != null)
                    layout.setWritables(((ArrayList<Integer>) rawLo.get("writables")));

                layouts.add(layout);
            }

        SystemTopologyStatus systemTopologyStatus = new SystemTopologyStatus();
        systemTopologyStatus.setDataCenters(dataCenters);
        systemTopologyStatus.setLayouts(layouts);
        systemTopologyStatus.setFree((Integer) ((Map) (map.get("Topology"))).get("Free"));
        systemTopologyStatus.setMax((Integer) ((Map) (map.get("Topology"))).get("Max"));
        systemTopologyStatus.setVersion((String) map.get("Version"));

        return systemTopologyStatus;
    }

    /**
     * Init cache manager and cache mapping.
     */
    private void initCache() {
        if (enableLookupVolumeCache) {
            CacheManagerBuilder builder = CacheManagerBuilder.newCacheManagerBuilder();
            this.cacheManager = builder.build(true);
            if (enableLookupVolumeCache)
                this.cacheManager.createCache(LOOKUP_VOLUME_CACHE_ALIAS,
                        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, LookupVolumeResult.class,
                                ResourcePoolsBuilder.heap(this.lookupVolumeCacheEntries))
                                .withExpiry(Expirations.timeToLiveExpiration(
                                        Duration.of(this.lookupVolumeCacheExpiry, TimeUnit.SECONDS))).build());
        }
    }

    /**
     * Close all cache and close cache manager.
     */
    private void closeCache() {
        if (cacheManager != null) {
            cacheManager.removeCache(LOOKUP_VOLUME_CACHE_ALIAS);
            cacheManager.close();
        }
    }

    private class PollSystemTopologyStatusThread extends Thread {
        private volatile boolean shutdown;

        public void run() {
            while (!shutdown) {
                synchronized (this) {
                    try {
                        Thread.sleep(volumeStatusCheckInterval * 1000);
                        systemTopologyStatus = fetchSystemTopologyStatus(leaderUrl);
//                        log.info("systemTopologyStatus:" + JSONObject.toJSON(systemTopologyStatus));
                    } catch (Exception ignored) {
                    }
                }
            }
        }

        private void shutdown() {
            this.shutdown = true;
            this.interrupt();
            synchronized (this) {
                notifyAll();
            }
        }
    }

    /**
     * Thread for cycle to check cluster status.
     */
    private class PollClusterStatusThread extends Thread {

        private volatile boolean shutdown;

        @Override
        public void run() {
            while (!shutdown) {
                synchronized (this) {
                    try {
                        Thread.sleep(statusExpiry * 1000);
                    } catch (InterruptedException ignored) {
                    }
                    updateSystemStatus();
                }
            }
        }

        void updateSystemStatus() {
            try {
                fetchSystemStatus(leaderUrl);
                connectionClose = false;
            } catch (IOException e) {
                connectionClose = true;
                log.error("unable connect to the target seaweedfs core [" + leaderUrl + "]");
            }

            try {
                if (connectionClose) {
                    log.info("lookup seaweedfs core leader by peers");
                    if (systemClusterStatus == null || systemClusterStatus.getPeers().size() == 0) {
                        log.error("cloud not found the seaweedfs core peers");
                    } else {
                        String url = findLeaderUriByPeers(systemClusterStatus.getPeers());
                        if (url != null) {
                            log.error("seaweedfs core cluster is failover");
                            fetchSystemStatus(url);
                            connectionClose = false;
                        } else {
                            log.error("seaweedfs core cluster is down");
                            systemClusterStatus.getLeader().setActive(false);
                            connectionClose = true;
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                log.error("unable connect to the seaweedfs core leader");
            }
        }

        private void fetchSystemStatus(String url) throws IOException {
            systemClusterStatus = fetchSystemClusterStatus(url);
//            systemTopologyStatus = fetchSystemTopologyStatus(url);
            if (!leaderUrl.equals(systemClusterStatus.getLeader().getUrl())) {
                leaderUrl = (systemClusterStatus.getLeader().getUrl());
                log.info("seaweedfs core leader is change to [" + leaderUrl + "]");
            }
//            log.info("systemTopologyStatus:"+JSONObject.toJSON(systemTopologyStatus));
//            log.info("systemClusterStatus:"+JSONObject.toJSON(systemClusterStatus));
        }

        private void shutdown() {
            this.shutdown = true;
            this.interrupt();
            synchronized (this) {
                notifyAll();
            }
        }

    }

    /**
     * Thread for close expired connections.
     */
    private class IdleConnectionMonitorThread extends Thread {

        private volatile boolean shutdown;

        @Override
        public void run() {
            try {
                while (!shutdown) {
                    synchronized (this) {
                        wait(statusExpiry);
                        // Close free connection
                        clientConnectionManager.closeExpiredConnections();
                        clientConnectionManager.closeIdleConnections(idleConnectionExpiry, TimeUnit.SECONDS);
                        log.debug("http client pool state [" + clientConnectionManager.getTotalStats().toString() + "]");
                    }
                }
            } catch (InterruptedException ignored) {
            }
        }

        void shutdown() {
            this.shutdown = true;
            this.interrupt();
            synchronized (this) {
                notifyAll();
            }
        }
    }
}
