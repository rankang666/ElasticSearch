package rk.elastic;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;
import rk.constants.Constants;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * @Author rk
 * @Date 2018/12/10 15:06
 * @Description:
 *
 *      准备数据:
 *      <doc>
 *         <url>http://gongyi.sohu.com/20120730/n349358066.shtml</url>
 *         <docno>fdaa73d52fd2f0ea-34913306c0bb3300</docno>
 *          <contenttitle>失独父母中年遇独子夭折　称不怕死亡怕养老生病</contenttitle>
 *          <content></content>
 *      </doc>
 *
 **/

class Article{
    private String url;
    private String docno;
    private String content;
    private String contenttitle;

    public Article() {
    }

    public Article(String url, String docno, String content, String contenttitle) {
        this.url = url;
        this.docno = docno;
        this.content = content;
        this.contenttitle = contenttitle;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDocno() {
        return docno;
    }

    public void setDocno(String docno) {
        this.docno = docno;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getContenttitle() {
        return contenttitle;
    }

    public void setContenttitle(String contenttitle) {
        this.contenttitle = contenttitle;
    }

    @Override
    public String toString() {
        return "Article{" +
                "url='" + url + '\'' +
                ", docno='" + docno + '\'' +
                ", content='" + content + '\'' +
                ", contenttitle='" + contenttitle + '\'' +
                '}';
    }
}

//  解析代码，取其中前20条
class XmlParser {
    public static List<Article> getArticle() {
        List<Article> list = new ArrayList<Article>();
        SAXReader reader = new SAXReader();
        Document document;
        try {
            document = reader.read(new File("news_sohusite_xml"));
            Element root = document.getRootElement();
            Iterator<Element> iterator = root.elementIterator("doc");
            Article article = null;
            int count = 0;
            while(iterator.hasNext()) {
                Element doc = iterator.next();
                String url = doc.elementTextTrim("url");
                String docno = doc.elementTextTrim("docno");
                String content = doc.elementTextTrim("content");
                String contenttitle = doc.elementTextTrim("contenttitle");
                article = new Article();
                article.setContent(content);
                article.setDocno(docno);
                article.setContenttitle(contenttitle);
                article.setUrl(url);
                if(++count > 20) {
                    break;
                }
                list.add(article);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

}





public class ElasticSearchTest2_3 {

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
        System.out.println(hostAndPorts);
        for (String hostAndPort : hostAndPorts.split(",")){
            String[] fields = hostAndPort.split(":");
            String host = fields[0];
            int port = Integer.valueOf(fields[1]);
            TransportAddress ts = new TransportAddress(new InetSocketAddress(host, port));
            client.addTransportAddresses(ts);
        }

        TransportAddress[] transportAddrs = new TransportAddress[3];
        client.addTransportAddresses(transportAddrs);
        System.out.println("cluster.name = " + client.settings().get("cluseter.name"));

    }

    String index = "search";
    //  批量导入ES库
    @Test
    public void bulkInsert() throws Exception {
        List<Article> list = XmlParser.getArticle();
        ObjectMapper oMapper = new ObjectMapper();
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for (int i = 0; i < list.size(); i++) {
            Article article = list.get(i);
            String val = oMapper.writeValueAsString(article);
            bulkRequestBuilder.add(new IndexRequest(index, "news",
                    article.getDocno()).source(val));
        }
        BulkResponse response = bulkRequestBuilder.get();
    }

    //查询
    @Test
    public void testSearch() {
        String indices = "bigdata";//指的是要搜索的哪一个索引库
        SearchRequestBuilder builder = client.prepareSearch(indices)
                .setSearchType(SearchType.DEFAULT)
                .setFrom(0)
                .setSize(5)//设置分页
                /**
                 * 这是最新的
                 * .highlighter(//设置高亮显示
                 *          SearchSourceBuilder.highlight()
                 *                      .field("address")
                 *                      .preTags("<font color='red' size='16px'>")
                 *                      .postTags("</font>")
                 *      )
                 */
                .addHighlightedField("name")//设置高亮字段
                .setHighlighterPreTags("<font color='blue'>")
                .setHighlighterPostTags("</font>");//高亮风格
        builder.setQuery( QueryBuilders.fuzzyQuery("name", "hadoop"));
        SearchResponse searchResponse = builder.get();
        SearchHits searchHits = searchResponse.getHits();
        SearchHit[] hits = searchHits.getHits();
        long total = searchHits.getTotalHits();
        System.out.println("总共条数：" + total);//总共查询到多少条数据
        for (SearchHit searchHit : hits) {

            Map<String, Object> source = searchHit.getSource();//这是最新的：searchHit.getSourceAsMap()
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            System.out.println("---------------------------");
            String name = source.get("name").toString();
            String author = source.get("author").toString();
            System.out.println("name=" + name);
            System.out.println("author=" + author);
            HighlightField highlightField = highlightFields.get("name");
            if(highlightField != null) {
                Text[] fragments = highlightField.fragments();
                name = "";
                for (Text text : fragments) {
                    name += text.toString();
                }
            }
            System.out.println("name: " + name);
            System.out.println("author: " + author);
        }
    }

}