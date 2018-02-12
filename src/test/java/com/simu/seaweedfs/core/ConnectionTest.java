

package com.simu.seaweedfs.core;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import com.simu.seaweedfs.FileSystemTest;

/**
 * @author DengrongGuan
 */
public class ConnectionTest {
    @Test
    public void forceGarbageCollection() throws Exception {
        connection.forceGarbageCollection();
    }

    @Test
    public void forceGarbageCollectionWithThreshold() throws Exception {
        connection.forceGarbageCollection(0.4f);
    }

    @Test
    public void preAllocateVolumes() throws Exception {
        connection.preAllocateVolumes(0, 0, 0, 2, null, null);
    }

    @Test
    public void getSystemClusterStatus() throws Exception {
        Assert.assertTrue(connection.getSystemClusterStatus().getLeader().isActive());
    }

    @Test
    public void getSystemTopologyStatus() throws Exception {
        Assert.assertTrue(connection.getSystemTopologyStatus().getMax() > 0);
    }

    @Test
    public void isConnectionClose() throws Exception {
        Assert.assertFalse(connection.isConnectionClose());
    }

    @Test
    public void getLeaderUrl() throws Exception {
        Assert.assertNotNull(connection.getLeaderUrl());
    }

    @Test
    public void getVolumeStatus() throws Exception {
        String dataNodeUrl =
                connection.getSystemTopologyStatus()
                        .getDataCenters().get(0).getRacks().get(0).getDataNodes().get(0).getUrl();
        Assert.assertEquals(dataNodeUrl,
                connection.getVolumeStatus(dataNodeUrl).getUrl());
    }

    private static Connection connection;

    @BeforeClass
    public static void setBeforeClass() throws Exception {
        FileSystemTest.startup();
        Thread.sleep(1000);
        connection = FileSystemTest.fileSource.getConnection();
    }

}