

package com.simu.seaweedfs;

import com.simu.seaweedfs.core.FileSource;

import java.io.IOException;

/**
 * @author ChihoSin modified by DengrongGuan
 */
public class FileSystemTest {

    public static FileSource fileSource;

    static {
        fileSource = new FileSource();
//        fileSource.setHost("0.0.0.0");
//        fileSource.setPort(9333);
    }

    public static void startup() throws IOException, InterruptedException {
        if (fileSource.getConnection() == null) {
            fileSource.startup();
            Thread.sleep(3000);
        }
    }

    public static void shutdown() {
        fileSource.shutdown();
    }

}
