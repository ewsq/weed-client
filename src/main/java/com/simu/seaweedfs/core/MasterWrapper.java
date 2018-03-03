package com.simu.seaweedfs.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.simu.seaweedfs.core.contect.*;
import org.apache.http.client.methods.HttpGet;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import com.simu.seaweedfs.core.http.JsonResponse;
import com.simu.seaweedfs.exception.SeaweedfsException;
import com.simu.seaweedfs.util.RequestPathStrategy;

import java.io.IOException;

import static com.simu.seaweedfs.core.Connection.LOOKUP_VOLUME_CACHE_ALIAS;

/**
 * Master server operation wrapper.
 *
 * @author ChihoSin modified by DengrongGuan
 */
public class MasterWrapper {

    private Connection connection;
    private Cache<Long, LookupVolumeResult> lookupVolumeCache;
    private ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Constructor.
     *
     * @param connection Connection from file source.
     */
    MasterWrapper(Connection connection) {
        this.connection = connection;
        final CacheManager cacheManager = connection.getCacheManager();
        if (cacheManager != null)
            this.lookupVolumeCache =
                    cacheManager.getCache(LOOKUP_VOLUME_CACHE_ALIAS, Long.class, LookupVolumeResult.class);
    }

    /**
     * Assign a file key.
     *
     * @param params Assign file key params.
     * @return Assign file key result.
     * @throws IOException Http connection is fail or server response within some error message.
     */
    AssignFileKeyResult assignFileKey(AssignFileKeyParams params) throws IOException {
        checkConnection();
        final String url = connection.getLeaderUrl() + RequestPathStrategy.assignFileKey + params.toUrlParams();
        HttpGet request = new HttpGet(url);
        JsonResponse jsonResponse = connection.fetchJsonResultByRequest(request);
        return objectMapper.readValue(jsonResponse.json, AssignFileKeyResult.class);
    }

    /**
     * Force garbage collection.
     *
     * @param params Force garbage collection params.
     * @throws IOException Http connection is fail or server response within some error message.
     */
    void forceGarbageCollection(ForceGarbageCollectionParams params) throws IOException {
        checkConnection();
        final String url = connection.getLeaderUrl() + RequestPathStrategy.forceGarbageCollection + params.toUrlParams();
        HttpGet request = new HttpGet(url);
        connection.fetchJsonResultByRequest(request);
    }

    /**
     * Pre-Allocate volumes.
     *
     * @param params pre allocate volumes params.
     * @return pre allocate volumes result.
     * @throws IOException Http connection is fail or server response within some error message.
     */
    PreAllocateVolumesResult preAllocateVolumes(PreAllocateVolumesParams params) throws IOException {
        checkConnection();
        final String url = connection.getLeaderUrl() + RequestPathStrategy.preAllocateVolumes + params.toUrlParams();
        HttpGet request = new HttpGet(url);
        JsonResponse jsonResponse = connection.fetchJsonResultByRequest(request);
        return objectMapper.readValue(jsonResponse.json, PreAllocateVolumesResult.class);
    }

    /**
     * Lookup volume.
     *
     * @param params Lookup volume params.
     * @return Lookup volume result.
     * @throws IOException Http connection is fail or server response within some error message.
     */
    LookupVolumeResult lookupVolume(LookupVolumeParams params) throws IOException {
        checkConnection();
        LookupVolumeResult result;
        if (this.lookupVolumeCache != null) {
            result = this.lookupVolumeCache.get(params.getVolumeId());
            if (result != null) {
                return result;
            } else {
                result = fetchLookupVolumeResult(params);
                this.lookupVolumeCache.put(params.getVolumeId(), result);
                return result;
            }
        } else {
            return fetchLookupVolumeResult(params);
        }
    }

    /**
     * Fetch lookup volume result.
     *
     * @param params fetch lookup volume params.
     * @return fetch lookup volume result.
     * @throws IOException Http connection is fail or server response within some error message.
     */
    private LookupVolumeResult fetchLookupVolumeResult(LookupVolumeParams params) throws IOException {
        checkConnection();
        final String url = connection.getLeaderUrl() + RequestPathStrategy.lookupVolume + params.toUrlParams();
        HttpGet request = new HttpGet(url);
        JsonResponse jsonResponse = connection.fetchJsonResultByRequest(request);
        return objectMapper.readValue(jsonResponse.json, LookupVolumeResult.class);
    }

    /**
     * Check connection is alive.
     *
     * @throws SeaweedfsException Http connection is fail.
     */
    private void checkConnection() throws SeaweedfsException {
        if (this.connection.isConnectionClose())
            throw new SeaweedfsException("connection is closed");
    }

}
