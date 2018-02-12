package com.simu.seaweedfs.core;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import com.simu.seaweedfs.FileSystemTest;
import com.simu.seaweedfs.core.file.FileHandleStatus;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * @author DengrongGuan
 */
public class FileTemplateTest {
    private static FileTemplate template;

    @BeforeClass
    public static void setBeforeClass() throws Exception {
        FileSystemTest.startup();
        template = new FileTemplate(FileSystemTest.fileSource.getConnection());
    }

    @Test
    public void getFileStream() throws Exception {
        FileHandleStatus fileHandleStatus = template.saveFileByStream("test.txt",
                new ByteArrayInputStream("@getFileStream".getBytes()));
        Assert.assertTrue(template.getFileStream(fileHandleStatus.getFileId()).getOutputStream()
                .toString().equals("@getFileStream"));
    }

    @Test
    public void getFileStatus() throws Exception {
        FileHandleStatus fileHandleStatus = template.saveFileByStream("test.txt",
                new ByteArrayInputStream("@getFileStatusHeader".getBytes()));
        Assert.assertTrue(template.getFileStatus(fileHandleStatus.getFileId()).getFileName().equals("test.txt"));
    }

    @Test
    public void getFileUrl() throws Exception {
        FileHandleStatus fileHandleStatus = template.saveFileByStream("test.txt",
                new ByteArrayInputStream("@getFileUrl".getBytes()));
        Assert.assertTrue(template.getFileUrl(fileHandleStatus.getFileId()).endsWith(fileHandleStatus.getFileId()));
    }

    @Test
    public void saveFileByStream() throws Exception {
        FileHandleStatus fileHandleStatus = template.saveFileByStream("test.txt",
                new ByteArrayInputStream("@saveFileByStream".getBytes()));
        Assert.assertTrue(fileHandleStatus.getSize() > 0);
    }

    @Test
    public void saveFilesByStreamMap() throws Exception {
        LinkedHashMap<String, InputStream> fileMap = new LinkedHashMap<String, InputStream>();
        fileMap.put("test_1.txt", new ByteArrayInputStream("@saveFilesByStreamMap_1".getBytes()));
        fileMap.put("test_2.txt", new ByteArrayInputStream("@saveFilesByStreamMap_2".getBytes()));
        fileMap.put("test_3.txt", new ByteArrayInputStream("@saveFilesByStreamMap_3".getBytes()));
        LinkedHashMap<String, FileHandleStatus> fileStatusMap = template.saveFilesByStreamMap(fileMap);
        for (String key : fileStatusMap.keySet()) {
            Assert.assertTrue(fileStatusMap.get(key).getSize() > 0);
        }
    }

    @Test
    public void deleteFile() throws Exception {
        FileHandleStatus status = template.saveFileByStream("test.txt",
                new ByteArrayInputStream("@deleteFile".getBytes()));
        template.deleteFile(status.getFileId());
    }

    @Test
    public void deleteFiles() throws Exception {
        LinkedHashMap<String, InputStream> map = new LinkedHashMap<String, InputStream>();
        map.put("test_1.txt", new ByteArrayInputStream("@saveFilesByStreamMap_1".getBytes()));
        map.put("test_2.txt", new ByteArrayInputStream("@saveFilesByStreamMap_2".getBytes()));
        map.put("test_3.txt", new ByteArrayInputStream("@saveFilesByStreamMap_3".getBytes()));
        LinkedHashMap<String, FileHandleStatus> reMap = template.saveFilesByStreamMap(map);
        ArrayList<String> fids = new ArrayList<String>();
        for (String name : reMap.keySet()) {
            fids.add(reMap.get(name).getFileId());
        }
        template.deleteFiles(fids);
    }

    @Test
    public void updateFileByStream() throws Exception {
        FileHandleStatus status = template.saveFileByStream("test.txt",
                new ByteArrayInputStream("@saveFileByStream".getBytes()));
        template.updateFileByStream(
                status.getFileId(), "test.txt", new ByteArrayInputStream("@updateFileByStream".getBytes()));
    }



}