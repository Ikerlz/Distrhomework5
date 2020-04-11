package HomeworkFive;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

/**
 * 按行分割文件 工具类
 * @author FX_SKY
 *
 */
public class Split {

    /**
     * @param args
     */
    public static void main(String[] args) {
        test();
    }

    private static void test() {
        //int startRowNumber = 0;
        //int rowNumberSize = 0;
        String dirPath = "input/";
        String sourceFileName = "news_tensite_xml.smarty.dat";

        for(int i=1;i<201;i++){
            int startRowNumber = (i-1)*6+1;
            int rowNumberSize = 6;
            splitFile(startRowNumber, rowNumberSize, dirPath, sourceFileName);
        }
    }
    /**
     * 从指定行数 startRowNumber起，截取 rowNumberSize 行保存到一个单独的文件，命名为 sourceFileName_1.后缀名
     * @param startRowNumber
     * @param rowNumberSize
     * @param dirPath
     * @param sourceFileName
     */
    public static void splitFile(int startRowNumber, int rowNumberSize,
                                 String dirPath, String sourceFileName) {

        File inputFile = new File(dirPath+sourceFileName);

        if(inputFile==null || !inputFile.exists()){
            throw new RuntimeException("待处理的数据文件不存在,文件路径:"+inputFile.getAbsolutePath());
        }

        System.out.println("开始分割文件  "+sourceFileName);

        String suffix = "";
        String realFileName = sourceFileName;
        int index = sourceFileName.lastIndexOf(".");

        if(index>0){
            //abc.txt
            suffix = ".txt";
            //suffix = sourceFileName.substring(index, sourceFileName.length());	//.txt
            realFileName = sourceFileName.substring(0, sourceFileName.lastIndexOf("."));	//abc
        }

        int serilizeNumber = 1;
        String outputPath = dirPath+realFileName+"_"+serilizeNumber + suffix;
        File outputFile = new File(outputPath);

        while(outputFile.exists()){	//判断分割后的文件是否已经存在

            serilizeNumber++;

            outputPath = dirPath+realFileName+"_"+serilizeNumber + suffix;
            outputFile = new File(outputPath);
        }

        System.out.println("分割后的文件 "+outputPath);

        int currentIndex = 0;	//当前的行数
        int writeNumber = 0;	//已经写出的行数

        InputStream in = null;
        InputStreamReader reader = null;
        BufferedReader br = null;

        OutputStream out = null;
        OutputStreamWriter writer = null;
        BufferedWriter bw = null;

        try {
            in = new FileInputStream(inputFile);
            reader = new InputStreamReader(in,"GBK");//指定编码
            br = new BufferedReader(reader);

            //写出
            out = new FileOutputStream(outputFile);
            writer = new OutputStreamWriter(out, "utf-8");
            bw = new BufferedWriter(writer);

            String line = null;
            String newLine = null;


            while((line=br.readLine())!=null){

                currentIndex++;

                if(writeNumber>=rowNumberSize){
                    break;
                }

                if(currentIndex>=startRowNumber){
                    newLine = line;

                    bw.write(newLine);
                    bw.newLine();

                    writeNumber++;
                }
            }

            bw.flush();

            System.out.println("分割文件完成...");

        }catch (Exception e) {
            e.printStackTrace();
        }finally{
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}