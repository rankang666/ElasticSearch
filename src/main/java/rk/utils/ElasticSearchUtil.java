package rk.utils;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import rk.constants.Constants;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

public class ElasticSearchUtil {

    private static TransportClient client;

    private ElasticSearchUtil(){}

    static{
        try {
            Properties properties = new Properties();
            InputStream in = ElasticSearchUtil.class.getClassLoader().getResourceAsStream("elasticsearch.conf");
            properties.load(in);
            Settings setting = Settings.builder()
                    .put(Constants.CLUSTER_NAME, properties.getProperty(Constants.CLUSTER_NAME))
                    .build();
            client = new PreBuiltTransportClient(setting);
            String hostAndPorts = properties.getProperty(Constants.CLUSTER_HOST_PORT);
            for (String hostAndPort : hostAndPorts.split(",")) {
                String[] fields = hostAndPort.split(":");
                String host = fields[0];
                int port = Integer.valueOf(fields[1]);
                TransportAddress ts = new TransportAddress(new InetSocketAddress(host, port));
                client.addTransportAddresses(ts);
            }
            System.out.println("cluster.name = " + client.settings().get("cluster.name"));
        }catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static TransportClient getTransportClient(){
       return client;
    }

    public static void close(TransportClient client){
        client.close();
    }

}
