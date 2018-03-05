package com.simu.seaweedfs;

import com.simu.seaweedfs.core.FileSource;
import com.simu.seaweedfs.core.FileTemplate;
import com.simu.seaweedfs.core.http.StreamResponse;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ChihoSin modified by DengrongGuan
 * @create 2018-02-07
 **/
public class Main {
    private static final String host="192.168.1.204";
    private static final int port = 9333;
    public static void main(String[] args) {
        //上传文件
        FileSource fileSource = new FileSource();
// SeaweedFS master server host
//        fileSource.setHost(host);
// SeaweedFS master server port
//        fileSource.setPort(port);
        List<String> urls = new ArrayList<>();
//        urls.add("192.168.1.204:9333");
//        urls.add("192.168.1.100:9333");
//        urls.add("192.168.1.200:9333");
        urls.add("localhost:9333");
        fileSource.setUrls(urls);
        fileSource.setConnectionTimeout(1000);
// Startup manager and listens for the change
        try {
            fileSource.startup();
        } catch (IOException e) {
            e.printStackTrace();
        }

//        String fromFile = "/Users/dengrongguan/Downloads/python-3.6.3-macosx10.6.pkg";
//        String toPath = "/Users/dengrongguan/Downloads/python";
//        long chunkSize = 5 * 1024 * 1024;
//        FileTemplate template = new FileTemplate(fileSource.getConnection());
//        try {
////            FileHandleStatus fileHandleStatus = template.saveLargeFile(fromFile, chunkSize, toPath);
////            System.out.println(fileHandleStatus.toString());
////            StreamResponse streamResponse = template.getFileStream("6,0105d9b2e7");
////            System.out.println(streamResponse.getOutputStream().toString());
//            template.saveFileByStream("test.txt",new FileInputStream(new File("/Users/dengrongguan/test.txt")));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }finally {
//            fileSource.shutdown();
//        }

//        File fromF = new File(fromFile);
//        String mime = new MimetypesFileTypeMap().getContentType(fromF);
//
//        ChunkManifest chunkManifest = new ChunkManifest();
//        chunkManifest.setName(fromF.getName());
//        chunkManifest.setSize(fromF.length());
//        chunkManifest.setMime(mime);
//
//        SplitFile splitFile = splitFile(fromFile,5 * 1024 * 1024, toPath);
//
//        FileTemplate template = new FileTemplate(fileSource.getConnection());
////        System.out.println(template.assignFileKeyResult().getFid());
//
//        List<FileBlock> blocks = splitFile.getBlocks();
//
//        List<ChunkInfo> chunkInfos = new ArrayList<ChunkInfo>();
//        for (FileBlock block : blocks) {
//            ChunkInfo chunkInfo = new ChunkInfo();
//            chunkInfo.setOffset(block.getOffset());
//            FileHandleStatus fileHandleStatus = template.saveFileByStream(block.getBaseName(), new FileInputStream(new File(block.getFullFilePath())));
//            chunkInfo.setFid(fileHandleStatus.getFileId());
//            chunkInfo.setSize(fileHandleStatus.getSize());
//            chunkInfos.add(chunkInfo);
//        }
//        chunkManifest.setChunks(chunkInfos);
//        String chunkManifestStr = JSON.toJSONString(chunkManifest);
//        System.out.println(chunkManifestStr);
//        FileHandleStatus fileHandleStatus = template.saveFileByString("test.txt",chunkManifestStr);
//        System.out.println(fileHandleStatus.toString());
//        fileSource.shutdown();
//        File file = new File("/Users/dengrongguan/Documents/menkor_file.csv");
        //
    }
}
