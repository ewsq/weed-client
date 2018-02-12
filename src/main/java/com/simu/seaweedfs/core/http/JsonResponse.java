

package com.simu.seaweedfs.core.http;

/**
 * @author DengrongGuan
 */
public class JsonResponse {

    public final String json;
    public final int statusCode;

    public JsonResponse(String json, int statusCode) {
        this.json = json;
        this.statusCode = statusCode;
    }
}
