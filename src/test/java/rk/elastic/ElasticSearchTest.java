package rk.elastic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rk.entity.BigdataProduct;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @Author rk
 * @Date 2018/12/10 12:15
 * @Description:
 *    学习elasticsearch的基本java api
 *      在5.x之前都是直接是TransportClient.builder
 *      在5.x之后都是直接是new PreBuiltTransportClient(Settings.EMPTY)
 *    Transport(jdbc中的connection)
 *
 **/
public class ElasticSearchTest {

    private static final String HOST="hadoop01,hadoop02,hadoop03";
    private static final int PORT=9300;
    private  TransportClient client;
    @Before
    public void setUp(){

        Settings setting = Settings.builder()
                .put("cluster.name","rk-ES")
                .build();
        client = new PreBuiltTransportClient(setting);
        TransportAddress[] transportAddrs = new TransportAddress[3];

        String[] hosts = HOST.split(",");
        for(int i = 0; i < hosts.length; i++){
            transportAddrs[i] = new TransportAddress(new InetSocketAddress(hosts[i],PORT));
        }
        client.addTransportAddresses(transportAddrs);

        //测试查看查看settings里面的配置，默认cluster.name = elasticsearch，
        // 所以在创建settings时，需要添加 .put("cluster.name","rk-ES")
       /* Settings sets = client.settings();
        Set<String> keys = sets.keySet();
        for (String key : keys){
            String value = sets.get(key);
            System.out.println("key: "  + key + "\t --->value: " + value);
        }*/




    }

    String INDEX = "product";
    String TYPE = "bigdata";

    /**
     * 在java api中添加索引的方式有4种：
     * json
     * map
     * object
     * XContentBuilder
     */

    @Test
    public void testAddJson(){
        String source = "{\"name\": \"hbase\",\"author\": \"Apache\",\"version\": \"1.1.5\"}";
        //添加一个索引
        IndexResponse response = client.prepareIndex(INDEX, TYPE, "1")
                .setSource(source,XContentType.JSON)
                .get();
        System.out.println("version: " + response.getVersion());
    }


    @Test
    public void testAddMap(){
        Map<String, Object> source = new HashMap<>();
        source.put("name","hbase");
        source.put("author","Apache");
        source.put("version", "1.1.5");
        //添加一个索引
        IndexResponse response = client.prepareIndex(INDEX, TYPE, "2")
                .setSource(source)
                .get();
        System.out.println("version: " + response.getVersion());
    }

    @Test
    public void testAddParams(){
        //添加一个索引
        IndexResponse response = client.prepareIndex(INDEX, TYPE, "3")
                .setSource("name", "flume", "author", "cloudera", "version", "1.8.0")
                .get();
        System.out.println("version: " + response.getVersion());
    }

    @Test
    public void testAddEntity() throws JsonProcessingException {
        BigdataProduct bp = new BigdataProduct("kafka", "linkedIn", "0.10.1.0");
        ObjectMapper om = new ObjectMapper();
        byte[] bytes = om.writeValueAsBytes(bp);
        //添加一个索引
        IndexResponse response = client.prepareIndex(INDEX, TYPE, "4")
                .setSource(bytes, XContentType.JSON)
                .get();
        System.out.println("version: " + response.getVersion());
    }

    @Test
    public void testAddXContent() throws IOException {
        //Create a new {@link XContentBuilder} using the given {@link XContent} content.
        XContentBuilder xContentBuilder = JsonXContent.contentBuilder()
                .startObject()
                .field("name", "sqoop")
                .field("author","apache")
                .field("version", "1.4.7")
                .endObject();
        //添加一个索引
        IndexResponse response = client.prepareIndex(INDEX, TYPE, "5")
                .setSource(xContentBuilder)
                .get();

        System.out.println("version: " + response.getVersion());
    }
    @Test
    public void testGet(){
        GetResponse response = client.prepareGet(INDEX, TYPE, "1").get();
        Map<String, Object> sources = response.getSource();
        System.out.println("version: " + response.getVersion());
        for (Map.Entry<String, Object> source : sources.entrySet()){
            System.out.println(source.getKey() + " = " + source.getValue());
        }

    }



    @Test
    public void testUpdate(){
        UpdateResponse response = client.prepareUpdate(INDEX, TYPE, "1")
                .setDoc("{\"version\": \"2.1.0\"}",XContentType.JSON)
                .get();
        testGet();
    }

    @Test
    public void testDelete(){
        DeleteResponse response = client.prepareDelete(INDEX, TYPE, "5")
                .get();
        System.out.println("version: " + response.getVersion());
    }

    @Test
    public void testBulk(){
        BulkResponse response = client.prepareBulk()
                .add(client.prepareIndex(INDEX, TYPE, "5").setSource("{\"name\":\"redis\", \"author\": \"redis\", \"version\": \"5.0.0\"}", XContentType.JSON))
                .add(client.prepareIndex(INDEX, TYPE, "6").setSource("{\"name\":\"scala\", \"author\": \"jvm\", \"version\": \"2.12.4\"}", XContentType.JSON))
                .add(client.prepareUpdate(INDEX, TYPE, "2").setDoc("{\"url\": \"http://flume.apache.org\"}", XContentType.JSON))
                .get();
        BulkItemResponse[] items = response.getItems();
        for (BulkItemResponse bir : items){
            System.out.println("id: " + bir.getId() + " ---> version: " + bir.getVersion());
        }

    }



    @After
    public void cleanUp(){
        client.close();
    }


}
