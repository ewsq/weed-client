

package com.simu.seaweedfs.core.topology;

import com.simu.seaweedfs.core.file.Size;
import com.simu.seaweedfs.util.ConnectionUtil;

/**
 * @author ChihoSin modified by DengrongGuan
 */
public class DataNode {

    private String url;
    private String publicUrl;
    private int volumes;
    private int free;
    private int max;
    private boolean active;
    private String dir;
    private Size maxSize;
    private Size freeSize;

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public Size getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(Size maxSize) {
        this.maxSize = maxSize;
    }

    public Size getFreeSize() {
        return freeSize;
    }

    public void setFreeSize(Size freeSize) {
        this.freeSize = freeSize;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = ConnectionUtil.convertUrlWithScheme(url);
    }

    public String getPublicUrl() {
        return publicUrl;
    }

    public void setPublicUrl(String publicUrl) {
        this.publicUrl = ConnectionUtil.convertUrlWithScheme(publicUrl);
    }

    public int getVolumes() {
        return volumes;
    }

    public void setVolumes(int volumes) {
        this.volumes = volumes;
    }

    public int getFree() {
        return free;
    }

    public void setFree(int free) {
        this.free = free;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }

    @Override
    public String toString() {
        return "DataNode{" +
                "url='" + url + '\'' +
                ", publicUrl='" + publicUrl + '\'' +
                ", volumes=" + volumes +
                ", free=" + free +
                ", max=" + max +
                ", active=" + active +
                ", dir=" + dir +
                ", maxSize=" + maxSize.getSize() + maxSize.getUnit().getName() +
                ", freeSize=" + freeSize.getSize() + freeSize.getUnit().getName() +
                '}';
    }
}
