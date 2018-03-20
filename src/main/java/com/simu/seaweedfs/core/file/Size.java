package com.simu.seaweedfs.core.file;

/**
 * @author DengrongGuan
 * @create 2018-03-19
 **/
public class Size {
    private long size;
    private SizeUnit unit;

    public Size() {
    }

    public Size(long size, SizeUnit unit) {
        this.size = size;
        this.unit = unit;
    }

    public double getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public SizeUnit getUnit() {
        return unit;
    }

    public void setUnit(SizeUnit unit) {
        this.unit = unit;
    }

    /**
     * 易于阅读的大小
     * @return
     */
    public Size human(){
        int index = unit.getIndex();
        while (size >= 1024 && index < 3){
            size /= 1024;
            index++;
        }
        this.unit = SizeUnit.getSizeUnitByIndex(index);
        return this;
    }

    /**
     * 单位转换
     * @param sizeUnit
     * @return
     */
    public Size transfer(SizeUnit sizeUnit){
        int toIndex = sizeUnit.getIndex();
        int fromIndex = this.unit.getIndex();
        while(fromIndex < toIndex){
            fromIndex++;
            this.size /= 1024;
        }
        while (fromIndex > toIndex){
            fromIndex--;
            this.size *= 1024;
        }
        this.unit = sizeUnit;
        return this;
    }
}
