package org.trueno.elasticsearch.plugin.service;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import com.corundumstudio.socketio.AckRequest;
import com.corundumstudio.socketio.Configuration;
import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.SocketIOServer;
import com.corundumstudio.socketio.listener.DataListener;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.Map;

public class APIService extends AbstractLifecycleComponent<APIService> {

    private SocketIOServer server;
    private String hostname;
    private String clusterName;
    private String pathHome;
    private int port;
    private Settings settings;
    private Node node;

    @Inject
    public APIService(final Settings settings) {
        super(settings);
        logger.info("CREATE APIService");

        this.port = Integer.parseInt(settings.get("trueno.api.port"));
        this.hostname = settings.get("trueno.api.hostname");
        this.pathHome = settings.get("path.home");
        this.clusterName = settings.get("cluster.name");
        this.settings = settings;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.info("START APIService");

        /* instantiate the configuration */
        Configuration config = new Configuration();

        /* set the listening hostname */
        config.setHostname(this.hostname);

        /* set the listening port */
        config.setPort(this.port);

        /* instantiate the server */
        final SocketIOServer server = new SocketIOServer(config);
        this.server = server;

        /* Instantiate service nodeClient */
        Settings.Builder b = NodeBuilder.nodeBuilder().settings();
        b.put("path.home",this.pathHome );
        this.node = NodeBuilder.nodeBuilder().settings(b).local(true).clusterName(this.clusterName).node();

        /* create the elasticSearch Client for our API */
        final ElasticClient eClient = new ElasticClient(node.client());

        /* set search event listener */
        server.addEventListener("search", SearchObject.class, new DataListener<SearchObject>() {
            @Override
            public void onData(SocketIOClient client, SearchObject data, AckRequest ackRequest) {
                //System.out.println(data);

                /* get time */
                //long startTime = System.currentTimeMillis();

                /* get results */
                Map<String,Object>[] results = eClient.search(data);

                /* print time */
                //long estimatedTime = System.currentTimeMillis() - startTime;

                //System.out.println("Execution time: " + estimatedTime +"ms");

                /* send back result */
                ackRequest.sendAckData(results);
            }
        });
        /* set bulk event listener */
        server.addEventListener("bulk", BulkObject.class, new DataListener<BulkObject>() {
            @Override
            public void onData(SocketIOClient client, BulkObject data, AckRequest ackRequest) {
                /* get results */
                String result = eClient.bulk(data);

                /* sending Acknowledge to socket client */
                ackRequest.sendAckData(result);
            }
        });

        /* starting socket server */
        server.startAsync().addListener(new FutureListener<Void>() {
            @Override
            public void operationComplete(Future<Void> future) throws Exception {
                if (future.isSuccess()) {
                    logger.info("Trueno ElasticSearch API Service started");
                } else {
                    logger.info("ERROR: Trueno ElasticSearch API Service unable to start");
                }
            }
        });

    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.info("STOP APIService");

        // TODO Your code..
        this.node.close();
        this.server.stop();
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        logger.info("CLOSE APIService");

        // TODO Your code..
    }

}
