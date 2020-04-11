package HomeworkFive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;

/**
 *   基于hanlp 分词 并计算 词频演示
 * @author mszhou
 */
public class test {

    //返回的分词对象
    class WordLabe{

        //分词
        private String label;

        //词频
        private int wordFrequency;

        public WordLabe() {}

        public WordLabe(String label, int wordFrequency) {
            this.label = label;
            this.wordFrequency = wordFrequency;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public int getWordFrequency() {
            return wordFrequency;
        }

        public void setWordFrequency(int wordFrequency) {
            this.wordFrequency = wordFrequency;
        }

    }


    /**
     * 获取分词并计算词频
     * @param content 需要分词的内容
     * @param topNum 需要返回前多少条分词
     * @return
     */
    public  List<WordLabe> wordDemoFun(String content,int topNum){
        //需要过滤的字符
        String filterStr = "`~!@#$^&*()=|{}':;',\\[\\].<>/?~！@#￥……&*（）——|{}【】‘；：”“'。，、？ ";
        //保存数据分词数据
        Map<String, WordLabe> extractLabelMap = new HashMap<String, WordLabe>(16);
        //计算分词
        List<Term> termList = HanLP.segment(content);
        //过滤一下字符
        List<String> list = termList.stream().map(a -> a.word).filter(s -> !filterStr.contains(s)).collect(Collectors.toList());

        //计算词频并封装ExtractLabelDto对象
        for (String word : list) {
            //判断不存在则新增(去重)
            if (extractLabelMap.get(word) == null) {
                //计算获取词频(获取word在list中重复的次数)
                int count = Collections.frequency(list, word);
                //封装成WordLabe对象
                extractLabelMap.put(word, new WordLabe(word, count));
            }
        }
        //map转list
        List<WordLabe> extractLabellist = new ArrayList<WordLabe>(extractLabelMap.values());
        //针对词频数量降序排序
        Collections.sort(extractLabellist, new Comparator<WordLabe>() {
            @Override
            public int compare(WordLabe o1, WordLabe o2) {
                //降序排序
                return o2.getWordFrequency() - o1.getWordFrequency();
            }
        });
        //如果大于topNum 则返回前 topNum 个
        return extractLabellist.size() > topNum ? extractLabellist.subList(0,topNum) : extractLabellist;

    }


    //测试
    public static void main(String[] args) {
        //List<WordLabe> list = new test().wordDemoFun("这是测试呀测试测呀斯卡萨",5);
        //System.out.println(list);


    }

}

