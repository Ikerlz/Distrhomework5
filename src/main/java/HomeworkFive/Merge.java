package HomeworkFive;

import java.io.File;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;

public class Merge {

    public static void main(String args[]) {
        merge();
    }

    private static void merge() {
        //int startRowNumber = 0;
        //int rowNumberSize = 0;
        String basepath = "input/news_tensite_xml.smarty_";
        //String sourceFileName = "news_tensite_xml.smarty.dat";

        for(int i=1;i<201;i++){
            String infilepath = basepath + i + ".txt";
            String outfilepath = "input/doc" + i + ".txt";
            MergeFile(infilepath, outfilepath);
        }
    }

    public static void MergeFile(String InputFileName, String OutputFileName) {

        try { // 防止文件建立或读取失败，用catch捕捉错误并打印，也可以throw

            /* 读入TXT文件 */

            File filename = new File(InputFileName); // 要读取以上路径的input。txt文件
            InputStreamReader reader = new InputStreamReader(
                    new FileInputStream(filename)); // 建立一个输入流对象reader
            BufferedReader br = new BufferedReader(reader); // 建立一个对象，它把文件内容转成计算机能读懂的语言
            String line = "";
            String res = "";
            line = br.readLine();
            while (line != null) {
                line = br.readLine(); // 一次读入一行数据
                res = res + line;
            }

            /* 写入Txt文件 */
            File writename = new File(OutputFileName); // 相对路径，如果没有则要建立一个新的output.txt文件
            writename.createNewFile(); // 创建新文件
            BufferedWriter out = new BufferedWriter(new FileWriter(writename));
            out.write(res); // 即为换行
            out.flush(); // 把缓存区内容压入文件
            out.close(); // 最后记得关闭文件

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

