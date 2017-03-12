package org.trueno.elasticsearch.plugin.service; /**
 * Created by Victor, Servio
 * ElasticSearch Client: includes connect(), search(), and bulk() operations
 */

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;

import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;

public class ElasticClient {

    /* Private properties */
    private Client client;
    private String clusterName;
    private String[] addresses;

    /**
     * Constructor
     * @param client -> Client
     */
    public ElasticClient(Client client) {
        /* set the node client */
        this.client = client;
    }

    /**
     * The search API allows you to execute a search query and get back search hits that match the query.
     * The query can either be provided using a simple query string as a parameter, or using a request body
     * @param data -> SearchObject
     * @return results -> ArrayList
     */
    public Map<String,Object>[] search(SearchObject data) {

        /* collecting results */
        ArrayList<Map<String,Object>> sources = new ArrayList<>();

        try{

            /* build query */
            SearchResponse resp =  this.client.prepareSearch(data.getIndex())
                    .setTypes(data.getType())
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                    .setSize(data.getSize())
                    .setQuery(data.getQuery()).get();

            SearchHit[] results = resp.getHits().getHits();

            /* for each hit result */
            for(SearchHit h: results){
            /* add map to array, note: a map is the equivalent of a JSON object */
                sources.add(h.getSource());
            }

            return sources.toArray(new Map[sources.size()]);

        }catch (Exception e){
            System.out.println(e);
        }
        return new Map[0];
    }

    /**
     * The bulk API allows one to index and delete several documents in a single request.
     * @param bulkData -> BulkObject [Index, Operations[][]]
     * @return [batch finished] -> String
     */
    public String bulk(BulkObject bulkData) {
        try {
            /* we will use this index instance on ES */
            String index = bulkData.getIndex();

            /* requested batch operations from client */
            String[][] operations = bulkData.getOperations();

            long totalStartTime = System.currentTimeMillis();

            BulkRequestBuilder bulkRequest = this.client.prepareBulk();

            for (String[] info : operations) {

                if (info[0].equals("index")) {
                    /*
                    info[0] = index or delete
                    info[1] = type {v, e}
                    info[2] = id
                    info[3] = '{name:pedro,age:15}'
                     */
//                    System.out.println("index: " + index);
//                    System.out.println("info[1]: " + info[1]);
//                    System.out.println("info[2]: " + info[2]);
//                    System.out.println("info[3]: " + info[3]);

                    System.out.println(index);
                    /* adding document to the batch */
                    bulkRequest.add(this.client.prepareIndex(index, info[1], info[2]).setSource(info[3]));

                    continue;
                }//if

                if (!info[0].equals("delete")) continue;

                /* adding document to the batch */
                bulkRequest.add(this.client.prepareDelete(index, info[1], info[2]));

            }//for

            BulkResponse bulkResponse = bulkRequest.get();

            long totalEstimatedTime = System.currentTimeMillis() - totalStartTime;

            System.out.println("batch time ms: " + totalEstimatedTime);

            if (bulkResponse.hasFailures()) {
                return bulkResponse.buildFailureMessage();
            }

            return "[]";
        }
        catch (Exception e) {
            e.printStackTrace(new PrintStream(System.out));
            return null;
        }
    }//bulk

}//class

