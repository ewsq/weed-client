package com.simu.seaweedfs.core.file;

/**
 * @author DengrongGuan
 * @create 2018-03-19
 **/
public enum SizeUnit {
    B(0,"B"),
    KB(1,"KB"),
    MB(2,"MB"),
    GB(3,"GB");

    private final int index;
    private final String name;
    SizeUnit(int index, String name){
        this.name = name;
        this.index = index;
    }

    public static SizeUnit getSizeUnitByIndex(int index){
        for (SizeUnit c : SizeUnit.values()) {
            if (c.index == index) {
                return c;
            }
        }
        return null;
    }

    public int getIndex(){
        return index;
    }

    public String getName(){
        return name;
    }

}
