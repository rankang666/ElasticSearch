package rk.elastic;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rk.constants.Constants;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;

/**
 * @Author rk
 * @Date 2018/12/10 15:06
 * @Description:
 **/
public class ElasticSearchTest2 {

    private TransportClient client;
    @Before
    public void setUp() throws IOException {
        Properties properties = new Properties();
        InputStream in = ElasticSearchTest2.class.getClassLoader().getResourceAsStream("elasticsearch.conf");
        properties.load(in);
        Settings setting = Settings.builder()
                .put(Constants.CLUSTER_NAME,properties.getProperty(Constants.CLUSTER_NAME))
                .build();
        client = new PreBuiltTransportClient(setting);
        String hostAndPorts = properties.getProperty(Constants.CLUSTER_HOST_PORT);
        for (String hostAndPort : hostAndPorts.split(",")){
            String[] fields = hostAndPort.split(":");
            String host = fields[0];
            int port = Integer.valueOf(fields[1]);
            TransportAddress ts = new TransportAddress(new InetSocketAddress(host, port));
            client.addTransportAddresses(ts);
        }
        System.out.println("cluster.name = " + client.settings().get("cluster.name"));
    }

    String[] indices = {"product","test"};

    @Test
    public void testQuery1(){
        SearchResponse response = client
                .prepareSearch(indices) // 指定要检索的索引库
                /**
                 * 设置检索方式：
                 *  QUERY_AND_FETCH:  在5.3之前，之后受保护
                 *  QUERY_THEN_FETCH:  默认
                 *  DFS_QUERY_AND_FETCH:  直接移除，新版本没有
                 *  DFS_QUERY_THEN_FETCH:
                 */
                .setSearchType(SearchType.DEFAULT)
                /**
                 * 设置要检索的内容
                 * 基于不同的检索方式，是否能够检索到想要的数据，就逐渐衍生出来了一个职位SEO，搜索引擎优化
                 */
//                .setQuery(QueryBuilders.matchPhrasePrefixQuery("firstname", "V*")) // 在firstname字段上检索以V开头的数据
//                .setQuery(QueryBuilders.matchQuery("state", "NM"))
                .setQuery(QueryBuilders.termQuery("age", 40))
                //分页，每页显示M条，显示第N页的数据setFrom((N - 1) * M ).setSize()
                .setFrom(1)//从哪一条开始显示
                .setSize(5)//每页显示的内容
                .get();
        // 返回检索结果数据，被封装SearchHits对象中
        SearchHits searchHits = response.getHits();
        long totalHits = searchHits.totalHits;
        System.out.println("搜索到"+totalHits+"个结果");

        /**
         * "hits": [
         * {
         * "_index": "product",
         * "_type": "bigdata",
         * "_id": "5",
         * "_score": 1,
         * "_source": {
         * "name": "redis",
         * "author": "redis",
         * "version": "5.0.0"
         * }
         * }
         */
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits){
            System.out.println("--------------------------------------------");
            String index = hit.getIndex();
            String type = hit.getType();
            String id = hit.getId();
            float score = hit.getScore();
            System.out.println("index: " + index);
            System.out.println("type: " + type);
            System.out.println("id: " + id);
            System.out.println("score: " + score);
            Map<String, Object> source = hit.getSourceAsMap();
            source.forEach((field, value) ->{
                System.out.println(field + "--->" + value);
            });

        }
    }

    @Test
    public void testHightLight(){
        SearchResponse response = client
                .prepareSearch(indices) // 指定要检索的索引库
                .setSearchType(SearchType.DEFAULT)
                .setQuery(QueryBuilders.matchQuery("address", "Avenue"))
                .highlighter(//设置高亮显示
                        SearchSourceBuilder.highlight()
                                .field("address")
                                .preTags("<font color='red' size='16px'>")
                                .postTags("</font>")
                )
                .setFrom(0)//从哪一条开始显示
                .setSize(5)//每页显示的内容
                .get();
        // 返回检索结果数据，被封装SearchHits对象中
        SearchHits searchHits = response.getHits();
        long totalHits = searchHits.totalHits;
        System.out.println("搜索到"+totalHits+"个结果");
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits){//获取高亮显示的内容
            System.out.println("-------------------------------------------");
            //高亮字段内容
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            highlightFields.forEach((key,highlightField) -> {
                System.out.println("key: " + key);
                String address = "";
                Text[] fragments = highlightField.fragments();
                for (Text fragment : fragments){
                    address += fragment.toString();
                }
                System.out.println("address: " + address);

            });


        }

    }

    @Test
    public void testSort(){
        SearchResponse response = client
                .prepareSearch(indices) // 指定要检索的索引库
                .setSearchType(SearchType.DEFAULT)
                .setQuery(QueryBuilders.matchQuery("address", "Avenue"))
                .highlighter(//设置高亮显示
                        SearchSourceBuilder.highlight()
                                .field("address")
                                .preTags("<font color='red' size='16px'>")
                                .postTags("</font>")
                )
                .addSort("age", SortOrder.ASC)
//                .addSort("age", SortOrder.DESC)
                .setFrom(0)//从哪一条开始显示
                .setSize(5)//每页显示的内容
                .get();
        // 返回检索结果数据，被封装SearchHits对象中
        SearchHits searchHits = response.getHits();
        long totalHits = searchHits.totalHits;
        System.out.println("搜索到"+totalHits+"个结果");
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits){//获取高亮显示的内容
            System.out.println("-------------------------------------------");
            Map<String, Object> source = hit.getSourceAsMap();
            Object firstname = source.get("firstname");
            Object age = source.get("age");
            System.out.println("firstname: " + firstname);
            System.out.println("age: " + age);
            //高亮字段内容
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            highlightFields.forEach((key,highlightField) -> {
                System.out.println("key: " + key);
                String address = "";
                Text[] fragments = highlightField.fragments();
                for (Text fragment : fragments){
                    address += fragment.toString();
                }
                System.out.println("address: " + address);
            });
        }

    }


    @Test
    public void testAggr(){
        SearchResponse response = client
                .prepareSearch(indices) // 指定要检索的索引库
                .setSearchType(SearchType.DEFAULT)
                .setQuery(QueryBuilders.matchQuery("address", "Avenue"))
                .addAggregation(
                        AggregationBuilders
                                .avg("avg_age")//select avg(age) avg_age --> 这里面的name就是最好显示的别名
                                .field("age")//select max(age), min(age), avg(age) avg_age --> 这里面的field就是这里的age对应列，或者索引库中的field
                )
                .get();
        Aggregations aggrs = response.getAggregations();//是个集合
//        System.out.println(aggrs);
        for (Aggregation aggr : aggrs){
//            System.out.println(aggr);
//            System.out.println(aggr.getName());
//            System.out.println(aggr.getType());
            InternalAvg avg = (InternalAvg) aggr;
            double value = avg.getValue();
            System.out.println(avg.getName() + "-->" + value);
        }
    }

    @Test
    public void testFilter(){
        SearchResponse response = client
                .prepareSearch(indices) // 指定要检索的索引库
                .setSearchType(SearchType.DEFAULT)
                .setQuery(QueryBuilders.matchQuery("address", "Avenue"))
                .highlighter(//设置高亮显示
                        SearchSourceBuilder.highlight()
                                .field("address")
                                .preTags("<font color='red' size='16px'>")
                                .postTags("</font>")
                )
                //过滤年龄在30~35之间的数据
                .setPostFilter(
                        QueryBuilders.rangeQuery("age").gte(30).lte(35)
                )
                .addSort("age", SortOrder.ASC)
                .setFrom(0)//从哪一条开始显示
                .setSize(5)//每页显示的内容
                .get();
        // 返回检索结果数据，被封装SearchHits对象中
        SearchHits searchHits = response.getHits();
        long totalHits = searchHits.totalHits;
        System.out.println("搜索到"+totalHits+"个结果");
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits){//获取高亮显示的内容
            System.out.println("-------------------------------------------");
            Map<String, Object> source = hit.getSourceAsMap();
            Object firstname = source.get("firstname");
            Object age = source.get("age");
            System.out.println("firstname: " + firstname);
            System.out.println("age: " + age);
            //高亮字段内容
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            highlightFields.forEach((key,highlightField) -> {
                System.out.println("key: " + key);
                String address = "";
                Text[] fragments = highlightField.fragments();
                for (Text fragment : fragments){
                    address += fragment.toString();
                }
                System.out.println("address: " + address);
            });
        }
    }


    @After
    public void cleanUp(){
        client.close();
    }

}
