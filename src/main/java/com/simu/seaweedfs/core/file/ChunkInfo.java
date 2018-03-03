package com.simu.seaweedfs.core.file;

/**
 * @author ChihoSin modified by DengrongGuan
 * @create 2018-01-23 下午8:46
 **/
public class ChunkInfo {
    private String Fid;
    private long Offset;
    private long Size;

    public String getFid() {
        return Fid;
    }

    public void setFid(String fid) {
        Fid = fid;
    }

    public long getOffset() {
        return Offset;
    }

    public void setOffset(long offset) {
        Offset = offset;
    }

    public long getSize() {
        return Size;
    }

    public void setSize(long size) {
        Size = size;
    }
}
