package rk.elastic;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rk.utils.ElasticSearchUtil;

import java.io.IOException;
import java.util.Map;

public class ElasticSearchTest3 {

    private TransportClient client;
    @Before
    public void setUp() throws IOException {
        client = ElasticSearchUtil.getTransportClient();
    }

    String[] indices = {"chinese"};
    @Test
    public void testChinese(){
        SearchResponse response = client
                .prepareSearch(indices) // 指定要检索的索引库
                .setSearchType(SearchType.DEFAULT)
//                .setQuery(QueryBuilders.matchQuery("content","中"))
//                .setQuery(QueryBuilders.termQuery("content","中"))
                .setQuery(QueryBuilders.termQuery("content","中国"))
                .get();
        // 返回检索结果数据，被封装SearchHits对象中
        SearchHits searchHits = response.getHits();
        long totalHits = searchHits.totalHits;
        System.out.println("搜索到"+totalHits+"个结果");
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

    @After
    public void cleanUp(){
        ElasticSearchUtil.close(client);
    }

}


