package com.iot.collect.send;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class LNGDataMergeSend {
    public static void main(String[] args) throws IOException {
        //文件夹路径
        String inDirPath = args[0];
        //输出文件路径
        String outPath = args[1];

        //获取目标文件夹下所有文件的Reader
        BufferedReader[] brs = getMergeFileReaders(inDirPath);
        //获取输出文件的Writer
        BufferedWriter bw = new BufferedWriter(new FileWriter(outPath));

        //合并排序输出
        timestampMergeSortOut(brs, bw, 0);

        //关闭所有Reader和资源
        closeReaders(brs);
        //关闭Writer资源
        bw.close();
    }

    private static BufferedReader[] getMergeFileReaders(String dirPath) {
        //获取目标文件夹中的所有文件对象
        File[] files = new File(dirPath).listFiles();
        //获取一个Reader集合
        List<BufferedReader> brList = new ArrayList<>();
        //定义用于存储每个文件路径的变量
        String filePath;
        for (int i = 0; i < files.length; ++i) {
            //保证加入集合中的文件都不为空
            if (files[i].exists() && files[i] != null && files[i].length() > 0) {
                filePath = files[i].getPath();
                try {
                    brList.add(new BufferedReader(new FileReader(new File(filePath))));
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
        //获取所有文件内容不为空的Reader数组
        BufferedReader[] brs = brList.toArray(new BufferedReader[brList.size()]);
        //清空Reader集合
        brList.clear();
        return brs;
    }

    private static void closeReaders(BufferedReader[] brs) {
        //依次关闭Reader资源
        for (int i = 0; i < brs.length; ++i) {
            try {
                if (brs[i] != null)
                    brs[i].close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 根据数组中每个Reader的每行数据的时间戳进行排序, 同时按时间戳间隔输出
     * @param brs 存储所有文件输入的字符流的数组
     * @param bw 文件输出的字符流
     * @param pos 时间戳的行中所在位置
     */
    public static void timestampMergeSortOut(BufferedReader[] brs, BufferedWriter bw, int pos)
            throws IOException {
        //获取要合并的文件数量
        int mergeNum = brs.length;
        //定义用于存储相邻两行的时间戳的变量: underTs >= preTs 恒成立
        long preTs = 0L;
        long underTs = 0L;

        //非空文件的数量，即每读完一个文件，这个变量减1
        int noEmpty = mergeNum;

        //定义一个字符串数组，用于存储每个文件读取的行数据
        String[] line = new String[mergeNum];
        for (int i = 0; i < mergeNum; ++i) {
            //先获取每个文件的第一行
            line[i] = brs[i].readLine();
        }
        //定义一个存储每个文件所在行的时间戳的数组
        long[] lineTs = new long[mergeNum];
        for (int i = 0; i < mergeNum; ++i) {
            //提取每个文件第一行中的时间戳
            lineTs[i] = (long) Double.parseDouble(line[i].split(",")[pos]);
        }

        //获取最小时间戳的位置, 即这个最小的时间戳来自于brs中哪个索引对应的文件
        int minTsPos = getMinIndex(lineTs);
        //输出所有文件中最小时间戳的行数据
        bw.write(line[minTsPos] + "\n");
        //立刻将一行数据刷写到文件中
        bw.flush();
        //将第一个时间戳作为preTs
        preTs = lineTs[minTsPos];

        //开始排序, 同时按照时间戳间隔发送行数据
        while (true) {
            if ((line[minTsPos] = brs[minTsPos].readLine()) != null ) {
                lineTs[minTsPos] = (long) Double.parseDouble(line[minTsPos].split(",")[pos]);
            } else {
                //为空说明当前文件已经读完, 非空文件数减1
                --noEmpty;
                lineTs[minTsPos] = Long.MAX_VALUE;
                if (noEmpty < 1) {
                    break;
                }
            }
            //更新minTsPos值，同时延迟输出下一行
            minTsPos = getMinIndex(lineTs);
            underTs = lineTs[minTsPos];
            //延迟输出数据
            delayOut(bw, line[minTsPos], underTs - preTs);
            //此时preTS变为当前的underTs，而underTs需要到下一轮重新获取
            preTs = underTs;
        }
    }

    /**
     * 延迟输出数据
     * @param bw 文件输出的字符流
     * @param line 输出的行数据
     * @param interval 延迟的时间
     */
    private static void delayOut(BufferedWriter bw, String line, long interval)
            throws IOException {
        try {
            //睡眠一段时间（这一段时间即为两行间的时间戳间隔）
            System.out.println(interval);
            Thread.sleep(interval);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //追加数据
        bw.write(line + "\n");
        //立即将一条数据刷写到文件中
        bw.flush();
    }

    //获取一个long型数组中的最小值对应的索引号
    private static int getMinIndex(long[] arr) {
        long minVal = arr[0];
        int minIndex = 0;
        for (int i = 0; i < arr.length; ++i) {
            if (arr[i] < minVal) {
                minVal = arr[i];
                minIndex = i;
            }
        }
        return minIndex;
    }

}
