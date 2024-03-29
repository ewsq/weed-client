package com.simu.seaweedfs.util;

import com.simu.seaweedfs.core.file.FileBlock;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * @author ChihoSin modified by DengrongGuan
 * @create 2018-01-23 下午8:20
 **/
public class FileSplitUtil {
    //文件的路径
    private String filePath;
    //块数
    private int size;
    //每块的大小
    private long blocksize;
    //文件名
    private String fileName;
    //文件大小
    private long length;
    //分割后的存放目录
    private String destBlockPath;
    //每块的名称
    private List<FileBlock> blocks;


    public FileSplitUtil() {
    }
    public FileSplitUtil(String filePath, String destBlockPath) {
        this(filePath,1024, destBlockPath);
    }

    public FileSplitUtil(String filePath, long blocksize, String destBlockPath) {
        this.filePath = filePath;
        this.destBlockPath = destBlockPath;
        this.blocksize = blocksize;
    }

    public List<FileBlock> getBlocks() {
        return blocks;
    }

    /**
     * 初始化操作 计算块数、确定文件名
     */
    public void init()
    {
        File src = null;
        //健壮性    创建成功就会得到构造方法初始化的值
        if(null==filePath || !(src=new File(filePath)).exists())
        {
            return;
        }
        if(src.isDirectory())
        {
            return;
        }
        File dest = new File(destBlockPath);
        if (!dest.exists()){
            dest.mkdir();
        }
        //文件名    g:/writer.txt的 writer.txt
        this.fileName = src.getName();
        //文件的大小
        this.length = src.length();

        //修正    每块的大小
        if(this.blocksize>length) //如果每块的大小大于文本的长度，则每块的大小=长度
        {
            this.blocksize = length;
        }

        //确定块数        ceil最小(最接近负无穷大)浮点值，该值大于等于该参数，并等于某个整数。
        size = (int) Math.ceil(length*1.0/this.blocksize);
        blocks = new ArrayList<FileBlock>(size) ;
    }

    /**
     * 文件分割
     * 确定在第几块
     * 1、起始位置
     * 2、实际大小
     * @throws IOException
     */
    public void split() throws IOException
    {
        List<FileBlock> fileBlocks = new ArrayList<>();

        long beginPos = 0;//起始点
        long actualBlockSize = blocksize;//实际大小
        //计算所有快的大小、位置、索引
        for(int i=0;i<size; i++)
        {
            if(i == size-1)
            {
                //最后一块
                actualBlockSize = this.length-beginPos;
            }
            //具体分割方法     第几块    起始分割地址        实际的块大小
            splitDetail(i,beginPos,actualBlockSize);
            beginPos += actualBlockSize;//本次的终点，下一次的起点
        }
    }

    /**
     * 文件分割  输入 输出
     * 文件拷贝
     * @param idx 第几块
     * @param beginPos 起始点
     * @param actualBlockSize 实际大小
     * @throws IOException
     */
    public void splitDetail(int idx,long beginPos,long actualBlockSize) throws IOException {
        FileBlock fileBlock = new FileBlock();
        String fullFilePath = destBlockPath+"/"+this.fileName+".part."+idx;
        fileBlock.setFullFilePath(fullFilePath);
        fileBlock.setOffset(beginPos);
        fileBlock.setBaseName(this.fileName+".part."+idx);
        //1、创建源
        File src = new File(this.filePath);//源文件
        //得到第几块的路径       List容器取出块路径
        File dest = new File(fullFilePath);//目标文件
        //2、选择流
        RandomAccessFile raf = null;//输入流
        BufferedOutputStream bos = null;//输出流
        try
        {
            raf = new RandomAccessFile(src, "r");
            bos = new BufferedOutputStream(new FileOutputStream(dest));
            //3、读取文件
            System.out.println("offset:"+beginPos);
            raf.seek(beginPos);
            //4、缓冲区
            byte[] flush = new byte[1024];
            int len = 0;
            while(-1!=(len=raf.read(flush)))
            {
                //写出
                if(actualBlockSize-len>=0)//判断是否足够
                {
                    bos.write(flush,0,len);//写出
                    actualBlockSize -= len;//剩余量
                }
                else
                {
                    //读取每一块实际大小的最后一小部分   最后一次写出
                    bos.write(flush,0,(int)actualBlockSize);
                    break;//每个block最后一部分读取完之后，一定要break，否则就会继续读取
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        finally{
            bos.close();
            raf.close();
        }
        this.blocks.add(fileBlock);
    }

    /**
     * 文件的合并   （方法一）
     */
    public void merge1(String destPath) throws Exception
    {
        //创建源
        File dest = new File(destPath);
        //选择流
        BufferedOutputStream bos = null;//输出流
        BufferedInputStream bis = null;//输入流
        try {
            bos = new BufferedOutputStream(new FileOutputStream(dest,true));//表示追加

            for(int i=0; i<this.blocks.size();i++)
            {
                //读取
                bis = new BufferedInputStream(new FileInputStream
                        (new File(this.blocks.get(i).getFullFilePath())));
                //缓冲区
                byte[] flush = new byte[1024];
                //接收长度
                int len = 0;
                while(-1 !=(len = bis.read(flush)))
                {
                    //打印到控制台
                    //System.out.println(new String(flush,0,len));
                    bos.write(flush,0,len);
                }
                bos.flush();
                bis.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally
        {
            bos.close();
        }
    }

    /**
     * 文件的合并  （方法二）
     */
    public void merge(String destPath) throws IOException
    {
        //1、创建源
        File dest = new File(destPath);

        //2、选择流
        //SequenceInputStream 表示其他输入流的逻辑串联。它从输入流的有序集合开始，
        //并从第一个输入流开始读取，直到到达文件末尾，接着从第二个输入流读取，依次类推，
        //直到到达包含的最后一个输入流的文件末尾为止。

        SequenceInputStream sis = null;//输入流
        BufferedOutputStream bos = null;//输出源

        //创建一个容器
        Vector<InputStream> vi = new Vector<InputStream>();
        for(int i=0; i<this.blocks.size();i++)
        {
            vi.add(new BufferedInputStream(
                    new FileInputStream(new File(this.blocks.get(i).getFullFilePath()))));
        }
        //SequenceInputStream sis = new SequenceInputStream(vi.elements());
        bos = new BufferedOutputStream(new FileOutputStream(dest,true));//表示追加
        sis = new SequenceInputStream(vi.elements());

        //缓冲区
        byte[] flush = new byte[1024];
        //接收长度
        int len = 0;
        while(-1 !=(len = sis.read(flush)))
        {
            //打印到控制台
            //System.out.println(new String(flush,0,len));
            bos.write(flush,0,len);
        }
        bos.flush();
        sis.close();
    }
}
