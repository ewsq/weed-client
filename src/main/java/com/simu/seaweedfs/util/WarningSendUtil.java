package com.simu.seaweedfs.util;

/**
 * @author DengrongGuan
 * @create 2018-03-20
 **/
public interface WarningSendUtil {
    void sendEmail(String content);
    void sendSMS(String content);
}
