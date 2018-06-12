

# SeaweedFS Client For Java
### pom.xml:  
```xml
<dependency>
    <groupId>com.simu.gdr</groupId>
    <artifactId>weed-client</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```
### application config  

```yml
weed:
  servers:
    urls: 192.168.1.204:9333,192.168.1.100:9333,192.168.1.200:9333
  auth:
    accessId: MtwtIlsti1P6UKDx
    accessKey: JN7fXj9sNPbLkfUTQh1zcJpwLPncHo
  subscriber:
    email: 15950570277@163.com
    phone: 15950570277
```
### Spring Bean Config  
```java
@Configuration
public class FileSourceConfig {

    @Bean("warningSendUtil")
    public WarningSendUtil getWarningSendUtil(@Value("${weed.subscriber.email}")String email, @Value("${weed.subscriber.phone}")String phone){
        return new WarningSendUtilImpl(email, phone);
    }

    @Bean("fileSource")
    public FileSource getFileSource(@Value("${weed.servers.urls}")String urls, @Qualifier("warningSendUtil")WarningSendUtil warningSendUtil) throws Exception{
        FileSource fileSource = new FileSource();
        fileSource.setUrls(Arrays.asList(urls.split(",")));
        fileSource.setConnectionTimeout(1000);
//        fileSource.setStatusExpiry(5);
        fileSource.setVolumeStatusCheckInterval(20 * 60); //volume节点状态每隔20分钟更新一次
        fileSource.setWarningSendUtil(warningSendUtil);
        fileSource.startup();
        return fileSource;
    }

    @Bean("fileTemplate")
    public FileTemplate getFileTemplate(@Qualifier("fileSource") FileSource fileSource){
        return new FileTemplate(fileSource.getConnection());
    }
}
```
### 在代码中使用，以上传文件为例，可以参考该组织的另一个项目dfs-proxy  
```java
@Autowired
FileTemplate fileTemplate;//注入bean

@Override
public void putFile(MultipartFile file, String path, String bucket) throws Exception {
    Bucket buck = bucketDao.getBucketByName(bucket);
    if (null == buck) {
        throw new ErrorCodeException(ResponseCodeEnum.BUCKET_NOT_EXIST);
    }
    String fileName = FileUtil.getSimpleFileName(path);
    String purePath = FileUtil.getPurePath(path);
    File fileEntity = fileDao.getFileByPath(buck.getId(), purePath, fileName);
    if (null != fileEntity) {
        // 删除原文件重新上传
        fileTemplate.deleteFile(fileEntity.getNumber());
        FileHandleStatus fileHandleStatus = fileTemplate.saveFileByStream(fileName, file.getInputStream());
        fileEntity.setNumber(fileHandleStatus.getFileId());
        fileEntity.setModifyTime(TimeUtil.getCurrentSqlTime());
        fileEntity.setSize(fileHandleStatus.getSize());
        fileEntity.update();
    } else {
        // assign key 单点故障;
        // 运行时有master宕机的情况(1. peer挂 2.leader挂) 主动发现和被动发现
        FileHandleStatus fileHandleStatus = fileTemplate.saveFileByStream(fileName, file.getInputStream(), bucket);
        fileEntity = new File(fileName, fileHandleStatus.getFileId(), purePath, fileHandleStatus.getSize(), buck.getId());
        fileEntity.setFolderId(createFolders(purePath, buck.getId()));
        fileEntity.save();
    }
}
```
