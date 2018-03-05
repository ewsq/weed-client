

package com.simu.seaweedfs.core;

import com.simu.seaweedfs.core.contect.AssignFileKeyParams;
import org.apache.http.entity.ContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import com.simu.seaweedfs.FileSystemTest;
import com.simu.seaweedfs.core.contect.AssignFileKeyResult;
import com.simu.seaweedfs.core.http.StreamResponse;

import java.io.ByteArrayInputStream;

/**
 * @author ChihoSin modified by DengrongGuan
 */
public class VolumeWrapperTest {
    private static MasterWrapper masterWrapper;
    private static VolumeWrapper volumeWrapper;

    @BeforeClass
    public static void setBeforeClass() throws Exception {
        FileSystemTest.startup();
        Thread.sleep(1000);
        FileSource manager = FileSystemTest.fileSource;
        volumeWrapper = new VolumeWrapper(manager.getConnection());
        masterWrapper = new MasterWrapper(manager.getConnection());
    }

    @Test
    public void checkFileExist() throws Exception {
        AssignFileKeyParams params = new AssignFileKeyParams();
        AssignFileKeyResult result = masterWrapper.assignFileKey(params);
        volumeWrapper.uploadFile(
                result.getUrl(),
                result.getFid(),
                "test.txt",
                new ByteArrayInputStream("@checkFileExist".getBytes()),
                null,
                ContentType.DEFAULT_BINARY);
        Assert.assertTrue(volumeWrapper.checkFileExist(result.getUrl(), result.getFid()));
    }

    @Test
    public void getFileStream() throws Exception {
        AssignFileKeyParams params = new AssignFileKeyParams();
        AssignFileKeyResult result = masterWrapper.assignFileKey(params);
        volumeWrapper.uploadFile(
                result.getUrl(),
                result.getFid(),
                "test.txt",
                new ByteArrayInputStream("@getFileContent".getBytes()),
                null,
                ContentType.DEFAULT_BINARY);

        StreamResponse cache = volumeWrapper.getFileStream(result.getUrl(), result.getFid());
        Assert.assertTrue(cache.getOutputStream().toString().equals("@getFileContent"));
    }

    @Test
    public void getFileStatusHeader() throws Exception {
        AssignFileKeyParams params = new AssignFileKeyParams();
        AssignFileKeyResult result = masterWrapper.assignFileKey(params);

        volumeWrapper.uploadFile(
                result.getUrl(),
                result.getFid(),
                "test.txt",
                new ByteArrayInputStream("@getFileStatusHeader".getBytes()),
                null,
                ContentType.DEFAULT_BINARY);

        Assert.assertTrue(
                volumeWrapper.getFileStatusHeader(result.getUrl(),
                        result.getFid()).getLastHeader("Content-Length").getValue().equals("44"));
    }

    @Test
    public void deleteFile() throws Exception {
        AssignFileKeyParams params = new AssignFileKeyParams();
        AssignFileKeyResult result = masterWrapper.assignFileKey(params);

        volumeWrapper.uploadFile(
                result.getUrl(),
                result.getFid(),
                "test.txt",
                new ByteArrayInputStream("@deleteFile".getBytes()),
                null,
                ContentType.DEFAULT_BINARY);

        volumeWrapper.deleteFile(result.getUrl(), result.getFid());
    }

    @Test
    public void uploadFile() throws Exception {
        AssignFileKeyParams params = new AssignFileKeyParams();
        AssignFileKeyResult result = masterWrapper.assignFileKey(params);

        Assert.assertTrue(
                volumeWrapper.uploadFile(
                        result.getUrl(),
                        result.getFid(),
                        "test.txt",
                        new ByteArrayInputStream("@uploadFile".getBytes()),
                        null,
                        ContentType.DEFAULT_BINARY) > 0L);
    }

}