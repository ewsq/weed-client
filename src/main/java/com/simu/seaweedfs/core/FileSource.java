

package com.simu.seaweedfs.core;

import com.simu.seaweedfs.core.topology.SystemClusterStatus;
import com.simu.seaweedfs.core.topology.SystemTopologyStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.cache.HttpCacheStorage;
import com.simu.seaweedfs.core.topology.VolumeStatus;
import com.simu.seaweedfs.exception.SeaweedfsException;
import com.simu.seaweedfs.util.ConnectionUtil;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Seaweed file system connection source.
 *
 * @author ChihoSin modified by DengrongGuan
 */
public class FileSource implements InitializingBean, DisposableBean {

    private static final Log log = LogFactory.getLog(FileSource.class);

    private List<String> urls = new ArrayList<>(); // host:port
    private int connectionTimeout = 10;
    private int statusExpiry = 5; // 每隔 5s 检查 master leader 的状态
    private int volumeStatusCheckInterval = 30 * 60;// 默认每隔30分钟检查一次volumes的状态
    private long volumeSizeWarnLimit = 1024 * 1024 * 1024;//默认剩余 1GB 开始报警
    private int maxConnection = 100;
    private int idleConnectionExpiry = 30;
    private int maxConnectionsPreRoute = 1000;
    private boolean enableLookupVolumeCache = true;
    private int lookupVolumeCacheExpiry = 120;
    private int lookupVolumeCacheEntries = 100;
    private boolean enableFileStreamCache = true;
    private int fileStreamCacheEntries = 1000;
    private long fileStreamCacheSize = 8192;
    private HttpCacheStorage fileStreamCacheStorage = null;
    private List<String> subscriberPhones = new ArrayList<>();
    private List<String> subscriberEmails = new ArrayList<>();
    volatile private boolean startup = false;

    private Connection connection;

    /**
     * Get wrapper connection.
     *
     * @return Wrapper connection.
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Start up the connection to the Seaweedfs server
     *
     * @throws IOException Http connection is fail or server response within some error message.
     */
    public void startup() throws IOException {
        String host = "";
        String port = "";
        for (String url: urls) {
            String httpUrl = ConnectionUtil.convertUrlWithScheme(url);
            if (ConnectionUtil.checkUriAlive(httpUrl)){
                String[] ss = url.split(":");
                host = ss[0];
                port = ss[1];
                break;
            }
        }
        if ("".equals(host)){
            throw new SeaweedfsException("cannot find any available server");
        }
        if (this.startup) {
            log.info("connect is already startup");
        } else {
            log.info("start connect to the seaweedfs master server [" +
                    ConnectionUtil.convertUrlWithScheme(host + ":" + port) + "]");
            if (this.connection == null) {
                this.connection = new Connection(
                        ConnectionUtil.convertUrlWithScheme(host + ":" + port),
                        this.connectionTimeout,
                        this.statusExpiry,
                        this.volumeStatusCheckInterval,
                        this.volumeSizeWarnLimit,
                        this.idleConnectionExpiry,
                        this.maxConnection,
                        this.maxConnectionsPreRoute,
                        this.enableLookupVolumeCache,
                        this.lookupVolumeCacheExpiry,
                        this.lookupVolumeCacheEntries,
                        this.enableFileStreamCache,
                        this.fileStreamCacheEntries,
                        this.fileStreamCacheSize,
                        this.fileStreamCacheStorage);
            }
            this.connection.startup();
            this.startup = true;
        }
    }

    /**
     * Shutdown connect to the any Seaweedfs server
     */
    public void shutdown() {
        log.info("stop connect to the seaweedfs master server");
        if (this.connection != null)
            this.connection.stop();
    }

    /**
     * Force garbage collection.
     *
     * @throws IOException Http connection is fail or server response within some error message.
     */
    public void forceGarbageCollection() throws IOException {
        connection.forceGarbageCollection();
    }

    /**
     * Force garbage collection.
     *
     * @param garbageThreshold Garbage threshold.
     * @throws IOException Http connection is fail or server response within some error message.
     */
    public void forceGarbageCollection(float garbageThreshold) throws IOException {
        connection.forceGarbageCollection(garbageThreshold);
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
    public void preAllocateVolumes(int sameRackCount, int diffRackCount, int diffDataCenterCount, int count, String dataCenter,
                                   String ttl) throws IOException {
        connection.preAllocateVolumes(sameRackCount, diffRackCount, diffDataCenterCount, count, dataCenter, ttl);
    }

    /**
     * Get master server  cluster status.
     *
     * @return Core cluster status.
     * @throws SeaweedfsException Connection is shutdown.
     */
    public SystemClusterStatus getSystemClusterStatus() throws SeaweedfsException {
        if (startup)
            return connection.getSystemClusterStatus();
        else
            throw new SeaweedfsException("Could not fetch server cluster status at connection is shutdown");
    }

    /**
     * Get cluster topology status.
     *
     * @return Core topology status.
     * @throws SeaweedfsException Connection is shutdown.
     */
    public SystemTopologyStatus getSystemTopologyStatus() throws SeaweedfsException {
        if (startup)
            return connection.getSystemTopologyStatus();
        else
            throw new SeaweedfsException("Could not fetch server cluster status at connection is shutdown");
    }

    /**
     * Check volume server status.
     *
     * @param volumeUrl Volume server url.
     * @return Volume server status.
     * @throws IOException Connection is shutdown or
     *                     Http connection is fail or server response within some error message.
     */
    public VolumeStatus getVolumeStatus(String volumeUrl) throws IOException {
        if (startup)
            return connection.getVolumeStatus(volumeUrl);
        else
            throw new SeaweedfsException("Could not fetch server cluster status at connection is shutdown");
    }

    /**
     * Working with Spring framework startup
     *
     * @throws IOException Http connection is fail or server response within some error message.
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        startup();
    }

    /**
     * Using when the Spring framework is destroy
     *
     * @throws IOException Http connection is fail or server response within some error message.
     */
    @Override
    public void destroy() throws Exception {
        shutdown();
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getStatusExpiry() {
        return statusExpiry;
    }

    public void setStatusExpiry(int statusExpiry) {
        this.statusExpiry = statusExpiry;
    }

    public int getVolumeStatusCheckInterval() {
        return volumeStatusCheckInterval;
    }

    public void setVolumeStatusCheckInterval(int volumeStatusCheckInterval) {
        this.volumeStatusCheckInterval = volumeStatusCheckInterval;
    }

    public List<String> getSubscriberPhones() {
        return subscriberPhones;
    }

    public void setSubscriberPhones(List<String> subscriberPhones) {
        this.subscriberPhones = subscriberPhones;
    }

    public List<String> getSubscriberEmails() {
        return subscriberEmails;
    }

    public void setSubscriberEmails(List<String> subscriberEmails) {
        this.subscriberEmails = subscriberEmails;
    }

    public int getMaxConnection() {
        return maxConnection;
    }

    public void setMaxConnection(int maxConnection) {
        this.maxConnection = maxConnection;
    }

    public int getMaxConnectionsPreRoute() {
        return maxConnectionsPreRoute;
    }

    public void setMaxConnectionsPreRoute(int maxConnectionsPreRoute) {
        this.maxConnectionsPreRoute = maxConnectionsPreRoute;
    }

    public boolean isEnableLookupVolumeCache() {
        return enableLookupVolumeCache;
    }

    public void setEnableLookupVolumeCache(boolean enableLookupVolumeCache) {
        this.enableLookupVolumeCache = enableLookupVolumeCache;
    }

    public int getLookupVolumeCacheExpiry() {
        return lookupVolumeCacheExpiry;
    }

    public void setLookupVolumeCacheExpiry(int lookupVolumeCacheExpiry) {
        this.lookupVolumeCacheExpiry = lookupVolumeCacheExpiry;
    }

    public int getIdleConnectionExpiry() {
        return idleConnectionExpiry;
    }

    public void setIdleConnectionExpiry(int idleConnectionExpiry) {
        this.idleConnectionExpiry = idleConnectionExpiry;
    }

    public int getLookupVolumeCacheEntries() {
        return lookupVolumeCacheEntries;
    }

    public void setLookupVolumeCacheEntries(int lookupVolumeCacheEntries) {
        this.lookupVolumeCacheEntries = lookupVolumeCacheEntries;
    }

    public boolean isEnableFileStreamCache() {
        return enableFileStreamCache;
    }

    public void setEnableFileStreamCache(boolean enableFileStreamCache) {
        this.enableFileStreamCache = enableFileStreamCache;
    }

    public int getFileStreamCacheEntries() {
        return fileStreamCacheEntries;
    }

    public void setFileStreamCacheEntries(int fileStreamCacheEntries) {
        this.fileStreamCacheEntries = fileStreamCacheEntries;
    }

    public long getVolumeSizeWarnLimit() {
        return volumeSizeWarnLimit;
    }

    public void setVolumeSizeWarnLimit(long volumeSizeWarnLimit) {
        this.volumeSizeWarnLimit = volumeSizeWarnLimit;
    }

    public long getFileStreamCacheSize() {
        return fileStreamCacheSize;
    }

    public void setFileStreamCacheSize(long fileStreamCacheSize) {
        this.fileStreamCacheSize = fileStreamCacheSize;
    }

    public HttpCacheStorage getFileStreamCacheStorage() {
        return fileStreamCacheStorage;
    }

    public void setFileStreamCacheStorage(HttpCacheStorage fileStreamCacheStorage) {
        this.fileStreamCacheStorage = fileStreamCacheStorage;
    }

    public List<String> getUrls() {
        return urls;
    }

    public void setUrls(List<String> urls) {
        this.urls = urls;
    }
}
