package com.simu.seaweedfs.util;


import org.junit.Test;

public class ConnectionUtilTest {
    @Test
    public void testCheckUriAlive() throws Exception {
    }

    @Test
    public void testCheckUriAlive1() throws Exception {
        if (!ConnectionUtil.checkUriAlive("http://192.168.1.204:9333")){
            System.out.println("down");
        }
        if (ConnectionUtil.checkUriAlive("http://192.168.1.100:9333")){
            System.out.println("ok");
        }

    }

    @Test
    public void testConvertUrlWithScheme() throws Exception {
    }

}