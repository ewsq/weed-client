

package com.simu.seaweedfs.core.contect;

import java.io.Serializable;

/**
 * @author DengrongGuan
 */
public class PreAllocateVolumesResult implements Serializable {

    private int count;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "PreAllocateVolumesResult{" +
                "count=" + count +
                '}';
    }
}
