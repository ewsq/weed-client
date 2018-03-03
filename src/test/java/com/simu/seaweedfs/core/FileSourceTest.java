

package com.simu.seaweedfs.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import com.simu.seaweedfs.FileSystemTest;

/**
 * @author ChihoSin modified by DengrongGuan
 */
public class FileSourceTest {

    private static final Log log = LogFactory.getLog(FileSourceTest.class);

    private static FileSource manager;

    @BeforeClass
    public static void setBeforeClass() throws Exception {
        FileSystemTest.startup();
        manager = FileSystemTest.fileSource;
    }

    @Test
    public void getSystemConnection() throws Exception {
        Assert.assertFalse(manager.getConnection().isConnectionClose());
        log.info("System Cluster:\n" + manager.getConnection().getSystemClusterStatus());
        log.info("System Topology:\n" + manager.getConnection().getSystemTopologyStatus());
    }

}