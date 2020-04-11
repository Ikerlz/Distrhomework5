package HomeworkFive;

// load packages
import java.util.Collections;
//import java.util.HashMap;
import java.util.List;
//import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;
import java.util.stream.Collectors;

//import com.hankcs.hanlp.tokenizer.StandardTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.hankcs.hanlp.HanLP;
//import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


public class InvertDoc {


    public static class Map extends Mapper<Object, Text, Text, IntWritable> {

        @Override

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            String line = value.toString();

            //利用正则表达式获取有用信息
            Pattern url_pattern = Pattern.compile("<url>(.*)</url>");
            Pattern title_pattern = Pattern.compile("<contenttitle>(.*)</contenttitle>");
            Pattern content_pattern = Pattern.compile("<content>(.*)</content>");

            Matcher url_match = url_pattern.matcher(line);
            Matcher title_match = title_pattern.matcher(line);
            Matcher content_match = content_pattern.matcher(line);

            String url = "";
            String title = "";
            String content = "";

            // 200条抓取的新闻中，有34条没有抓取到内容，即<content>标签的内容为空

            while (url_match.find() & title_match.find() & content_match.find()) {
                /*url = url_match.group().substring(5, url_match.group().length()-6);
                title = title_match.group().substring(15, title_match.group().length()-16);
                content = content_match.group().substring(10, content_match.group().length()-11);

                 */
                String Url = url_match.group();
                String Title = title_match.group();
                String Content = content_match.group();

                url = Url.substring(5, Url.length()-6);
                title = Title.substring(14, Title.length()-15);
                content = Content.substring(9, Content.length()-10);

            }

            // 对content进行分词

            //需要过滤的字符
            String filterStr = "`~!@#$^&*()=|{}':;',\\[\\].<>/?~！@#￥……&*（）·×÷’——|{}【】———……〔〕　°…℃《》‘；：”“'。，、？ ";
            //保存数据分词数据
            //java.util.Map<String, test.WordLabe> extractLabelMap = new HashMap<String, test.WordLabe>(16);
            //计算分词
            List<Term> termList = HanLP.segment(content);
            //过滤一下字符
            List<String> list = termList.stream().map(a -> a.word).filter(s -> !filterStr.contains(s)).collect(Collectors.toList());

            //Segment segment = HanLP.newSegment();
            //List<Term> termList = StandardTokenizer.segment(content);
            //计算词频并封装ExtractLabelDto对象
            int max_freq = 0;
            for (String word : list) {
                int count = Collections.frequency(list, word);
                if (count > max_freq) {
                    max_freq = count;
                }
            }
            for (String word : list) {
                //计算获取词频(获取word在list中重复的次数)
                context.write(new Text(word+"@"+title+"#"+url+"&&&"+max_freq), new IntWritable(1));
            }


            /*System.out.println(url);
            System.out.println(title);
            System.out.println(content);
             */


            //Text word = new Text();
            //Text filename_url = new Text();
            //String url = "";
            //String filename = "";
            /*if (line.substring(0,5).equals("<url>")) {
                String url = line.substring(5, line.length()-5);
            }
            if (line.substring(0,14).equals("<contenttitle>")) {
                String filename = line.substring(14, line.length()-14);
            }

             */

            //System.out.println(url);
            //System.out.println(filename);
            /*Text fileName_lineOffset = new Text(fileName+"@"+key.toString());
            StringTokenizer itr = new StringTokenizer(value.toString());
            for(; itr.hasMoreTokens(); ) {
                word.set(itr.nextToken());
                context.write(word, fileName_lineOffset);
            }

             */
        }
    }



    public static class Combine extends Reducer<Text, IntWritable, Text, IntWritable> {
        /**
         * 将Map输出的中间结果相同key部分的value累加，减少向Reduce节点传输的数据量
         * 输出：key:word@title#url, value:累加和
         */
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum ++;
            }
            context.write(key, new IntWritable(sum));
        }
    }


    public static class Partition extends HashPartitioner<Text, IntWritable> {
        //基于哈希值的分片方法
        /**
         * 为了将同一个word的键值对发送到同一个Reduce节点，对key进行临时处理
         * 将原key的(word, title@url)临时拆开，使Partitioner只按照word值进行选择Reduce节点
         */
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            //第三个参数numPartitions表示每个Mapper的分片数，也就是Reducer的个数
            String term = key.toString().split("@")[0];//获取word@title@url中的word
            return super.getPartition(new Text(term), value, numReduceTasks);//按照word分配reduce节点
        }
    }



    /*public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Iterator<Text> it = values.iterator();
            StringBuilder all = new StringBuilder();
            if(it.hasNext()) all.append(it.next().toString());
            for(; it.hasNext(); ) {
                all.append(";");
                all.append(it.next().toString());
            }
            context.write(key, new Text(all.toString()));
        }
    }

     */


    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        private String lastfile = null;//存储上一个filename
        private String lastword = null;//存储上一个word
        private String str = "";//存储要输出的value内容
        private int count = 0;
        private int totalcount = 0;
        //private StringBuilder out = new StringBuilder();//临时存储输出的value部分
        /**
         * 利用每个Reducer接收到的键值对中，word是排好序的
         * 将word@title#url拆分开，将title#url与累加和拼到一起，存在str中
         * 每次比较当前的word和上一次的word是否相同，若相同则将filename和累加和附加到str中
         * 否则输出：key:word，value:str
         * 并将新的word作为key继续
         * 输入：key:word#filename, value:[NUM,NUM,...]
         * 输出：key:word, value:filename:NUM;filename:NUM;...
         */
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String[] tokens = key.toString().split("@");//将word和title#url存在tokens数组中
            if(lastword == null) {
                lastword = tokens[0];
            }
            if(lastfile == null) {
                lastfile = tokens[1];
            }
            if (!tokens[0].equals(lastword)) {//此次word与上次不一样，则将上次的word进行处理并输出
                str += "<"+lastfile+","+count+">;<total,"+totalcount+">.";
                context.write(new Text(lastword), new Text(str));//value部分拼接后输出
                lastword = tokens[0];//更新word
                lastfile = tokens[1];//更新filename
                count = 0;
                str="";
                for (IntWritable val : values) {//累加相同word和filename中出现次数
                    count += val.get();//转为int
                }
                totalcount = count;
                return;
            }

            if(!tokens[1].equals(lastfile)) {//新的文档
                str += "<"+lastfile+","+count+">;";
                lastfile = tokens[1];//更新文档名
                count = 0;//重设count值
                for (IntWritable value : values){//计数
                    count += value.get();//转为int
                }
                totalcount += count;
                return;
            }

            //其他情况，只计算总数即可
            for (IntWritable val : values) {
                count += val.get();
                totalcount += val.get();
            }
        }

        /**
         * 上述reduce()只会在遇到新word时，处理并输出前一个word，故对于最后一个word还需要额外的处理
         * 重载cleanup()，处理最后一个word并输出
         */
        public void cleanup(Context context) throws IOException, InterruptedException {
            str += "<"+lastfile+","+count+">;<total,"+totalcount+">.";
            context.write(new Text(lastword), new Text(str));

            super.cleanup(context);
        }
    }


    //public static class Map extends Mapper<LongWritable,Text,Text,IntWritable> {

        //public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
        //Text k1, Text v1, Context context
            /*String line1 = value.toString(); //读取一行数据
            String line2 = value.toString(); //读取一行数据
            String line3 = value.toString(); //读取一行数据
            String line4 = value.toString(); //读取一行数据
            String line5 = value.toString(); //读取一行数据
            String line6 = value.toString(); //读取一行数据

            String line = line1 + line2 + line3 + line4 + line5 + line6;
            System.out.println(line);
            System.out.println("+++++++++++++++++++++++++++++++=");

             */
            //Segment segment = HanLP.newSegment();
            /*String Line = new String(value.getBytes(), 0, value.getLength(), "GBK"); //读取一行数据
            String line = Line.replaceAll("　", "");
            List<Term> termList = StandardTokenizer.segment(line);
            for (Term term:termList){
                System.out.println(term.word);
            }
             */


            //System.out.println(line);
            /*if (line.equals("<doc>")){
                String title = "";
                String Content = "";
            } else if (line.substring(0,14).equals("<contenttitle>")){
                //Pattern r = Pattern.compile("<contenttitle>(.*)</contenttitle>");
                String title = line.substring(14, line.length() -15);
                System.out.println(title);
            } else if (line.substring(0,9).equals("<content>")){
                //Pattern r = Pattern.compile("<content>(.*)</content>");
                String content = line.substring(9, line.length() -10);
                System.out.println(content);
            } else if (line.equals("</doc>")){
                System.out.println(line);
            }

             */

            //String str[] = line.split(" "); //用“ ”分隔符将一行数据分割并存在数组中
            //String file = str[str.length - 1]; // 最后一个元素为文件名
            //String filetype = file.split("\\.")[1]; // 文件类型
            //context.write(new Text(line), new IntWritable(1)); // map操作
        //}
    //}


    /*public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException,InterruptedException{

            int count = 0;
            for(IntWritable value: values) {
                count += value.get();
            }
            context.write(key, new IntWritable(count));
            context.write(key, new IntWritable(count));

        }
    }

     */

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "InvertDoc");//设置环境参数
        job.setJarByClass(InvertDoc.class);//设置整个程序的类名
        job.setMapperClass(InvertDoc.Map.class);//设置Mapper类
        job.setCombinerClass(InvertDoc.Combine.class);//设置combiner类
        job.setPartitionerClass(InvertDoc.Partition.class);//设置Partitioner类
        job.setReducerClass(InvertDoc.Reduce.class);//设置reducer类
        job.setOutputKeyClass(Text.class);//设置Mapper输出key类型
        job.setOutputValueClass(IntWritable.class);//设置Mapper输出value类型
        FileInputFormat.addInputPath(job, new Path(args[0]));//输入文件目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//输出文件目录
        System.exit(job.waitForCompletion(true) ? 0 : 1);//参数true表示检查并打印 Job 和 Task 的运行状况


    }
}
