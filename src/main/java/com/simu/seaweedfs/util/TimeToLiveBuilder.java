

package com.simu.seaweedfs.util;

/**
 * @author ChihoSin modified by DengrongGuan
 */
public class TimeToLiveBuilder {

    public static String buildMinutes(int count) {
        return String.valueOf(count) + "m";
    }

    public static String buildHours(int count) {
        return String.valueOf(count) + "h";
    }

    public static String buildDays(int count) {
        return String.valueOf(count) + "d";
    }

    public static String buildWeeks(int count) {
        return String.valueOf(count) + "w";
    }

    public static String buildMonths(int count) {
        return String.valueOf(count) + "M";
    }

    public static String buildYears(int count) {
        return String.valueOf(count) + "y";
    }

}
