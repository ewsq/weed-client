package com.simu.seaweedfs.core.file;

/**
 * @author DengrongGuan
 * @create 2018-01-25 下午7:07
 **/
public class FileBlock {
    private String baseName;
    private String fullFilePath;
    private long offset;

    public String getBaseName() {
        return baseName;
    }

    public void setBaseName(String baseName) {
        this.baseName = baseName;
    }

    public String getFullFilePath() {
        return fullFilePath;
    }

    public void setFullFilePath(String fullFilePath) {
        this.fullFilePath = fullFilePath;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
