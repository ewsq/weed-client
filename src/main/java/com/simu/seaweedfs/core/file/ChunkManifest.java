package com.simu.seaweedfs.core.file;

import java.util.List;

/**
 * @author DengrongGuan
 * @create 2018-01-25 下午4:30
 **/
public class ChunkManifest {
    private String Name;
    private long Size;
    private String Mime;
    private List<ChunkInfo> Chunks;

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public long getSize() {
        return Size;
    }

    public void setSize(long size) {
        Size = size;
    }

    public String getMime() {
        return Mime;
    }

    public void setMime(String mime) {
        Mime = mime;
    }

    public List<ChunkInfo> getChunks() {
        return Chunks;
    }

    public void setChunks(List<ChunkInfo> chunks) {
        Chunks = chunks;
    }
}
