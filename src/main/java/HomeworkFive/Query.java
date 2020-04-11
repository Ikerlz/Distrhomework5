package HomeworkFive;

// load packages

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Query {

    public static String[] TF_IDF(String line){
        String[] LineSplitArray =line.split(";");

        Pattern pattern = Pattern.compile("<(.*)>");
        Matcher match = pattern.matcher(LineSplitArray[0]);
        while (match.find()) {
            LineSplitArray[0] = match.group();
        }

        // 先算IDF，总文档数为200
        float IDF = (float) Math.log(200.0/(LineSplitArray.length-1));


        float[] TF_IDF_Value = new float[LineSplitArray.length-1];
        HashMap<String, String> doc_information = new HashMap<>();

        for (int i=0; i<LineSplitArray.length; i++) {
            String item = LineSplitArray[i];
            Pattern pattern_item = Pattern.compile("&&&(.*)>");
            Pattern pattern_title_url = Pattern.compile("<(.*)&&&");
            Matcher match_item = pattern_item.matcher(item);
            Matcher match_title_url = pattern_title_url.matcher(item);

            while (match_item.find() & match_title_url.find()) {
                String[] maxtf_and_tf = match_item.group().substring(3, match_item.group().length()-1).split(",");
                float TF = Float.parseFloat(maxtf_and_tf[1])/Float.parseFloat(maxtf_and_tf[0]);
                final double v = Math.random()/100000; //增加一个随机数，不影响结果，但能区别开相同的值
                float tf_idf = TF * IDF - (float)v;
                String title_url = match_title_url.group().substring(1, match_title_url.group().length()-3);

                TF_IDF_Value[i] = tf_idf;
                doc_information.put(Float.toString(tf_idf), title_url);
            }
        }
        Arrays.sort(TF_IDF_Value); //降序

        String[] sort_document = new String[LineSplitArray.length-1];
        for (int i=0; i < LineSplitArray.length-1; i++) {
            sort_document[i] = doc_information.get(Float.toString(TF_IDF_Value[LineSplitArray.length-2-i])) + "#" + Float.toString(TF_IDF_Value[LineSplitArray.length-2-i]);
        }

        return sort_document;

    }



    public static void main(String[] args) throws IOException {
        // 交互界面
        String keyword;
        System.out.println("****************************************");
        System.out.println("欢迎使用，请手动输入需要查询的关键字或词进行查询");
        System.out.println("****************************************\n");
        Scanner sc=new Scanner(System.in);
        System.out.println("关键词：");
        keyword = sc.next();
        System.out.println("正在为您查找，请稍后...\n");
        System.out.println("*****************");


        File file=new File("doc.txt");
        BufferedReader reader=null;
        String temp=null;

        reader = new BufferedReader(new FileReader(file));


        while((temp = reader.readLine()) != null) {
            if (temp.substring(0,keyword.length()).equals(keyword)){
                String[] res = (TF_IDF(temp));
                System.out.println("共为您搜索到" + res.length + "篇文章");
                System.out.println("*****************\n");
                System.out.println("排名" + "                     " +
                        "文章标题" + "                                               " +
                        "网页链接" + "                                            " + "     TF-IDF值");
                int rank = 1;
                for (String item : res){
                    //System.out.println(item);
                    String[] item_split =item.split("#");
                    System.out.println(rank + "     ||     " +
                            item_split[0] + "     ||     " +
                            item_split[1] + "     ||     " +
                            item_split[2]);
                    rank = rank + 1;
                }
                break;
            }
        }
        if (temp==null) {
            System.out.println("非常抱歉，没有相关文章包含该关键词");
        }

        /*while((temp = reader.readLine()).substring(0,2).equals("一万")) {
            System.out.println(temp);
        }

         */


    }



}
