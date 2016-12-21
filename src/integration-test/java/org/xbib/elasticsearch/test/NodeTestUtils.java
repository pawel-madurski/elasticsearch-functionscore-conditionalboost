package org.xbib.elasticsearch.test;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.junit.After;
import org.junit.Before;
import org.xbib.elasticsearch.plugin.condboost.CondBoostPlugin;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class NodeTestUtils {

    protected static String indexName = "es-functionscore-condboost-test";

    private final static Logger logger = ESLoggerFactory.getLogger(indexName);

    private Map<String, Node> nodes = new HashMap<>();

    private Map<String, AbstractClient> clients = new HashMap<>();

    private AtomicInteger counter = new AtomicInteger();

    private String cluster;

    private String host;

    private int port;

    @Before
    public void setUp() {
        try {
            logger.info("starting");
            setClusterName();
            startNode("1");
            findNodeAddress();
            try {
                ClusterHealthResponse healthResponse = client("1").execute(ClusterHealthAction.INSTANCE,
                        new ClusterHealthRequest().waitForYellowStatus().timeout(TimeValue.timeValueSeconds(30))).actionGet();
                if (healthResponse != null && healthResponse.isTimedOut()) {
                    healthResponse = client("1").execute(ClusterHealthAction.INSTANCE,
                            new ClusterHealthRequest().waitForGreenStatus().timeout(TimeValue.timeValueSeconds(30))).actionGet();
                    if (healthResponse != null && healthResponse.isTimedOut()) {
                        throw new IOException("cluster state is " + healthResponse.getStatus().name()
                                + ", from here on, everything will fail!");
                    }
                }
            } catch (ElasticsearchTimeoutException e) {
                throw new IOException("timeout, cluster does not respond to health request, cowardly refusing to continue with operations");
            }
        } catch (Throwable t) {
            logger.error("startNodes failed", t);
        }
    }

    @After
    public void tearDown() {
        try {
            closeNodes();
        } catch (Exception e) {
            logger.error("can not close nodes", e);
        } finally {
            try {
                deleteFiles();
                logger.info("data files wiped");
                Thread.sleep(2000L);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    protected void setClusterName() {
        this.cluster = "test-functionscore-conditionalboost-"
                + "-" + System.getProperty("user.name")
                + "-" + counter.incrementAndGet();
    }

    protected String getClusterName() {
        return cluster;
    }

    protected Settings getSettings() {
        return Settings.builder()
                .put("host", host)
                .put("port", port)
                .put("cluster.name", cluster)
                .put("path.home", getHome())
                .build();
    }

    protected Settings getNodeSettings() {
        return Settings.builder()
                .put("cluster.name", cluster)
                //.put("cluster.routing.schedule", "50ms")
                .put("cluster.routing.allocation.disk.threshold_enabled", false)
                //.put("discovery.zen.multicast.enabled", true)
                //.put("discovery.zen.multicast.ping_timeout", "5s")
                .put("http.enabled", true)
                //.put("threadpool.bulk.size", Runtime.getRuntime().availableProcessors())
                //.put("threadpool.bulk.queue_size", 16 * Runtime.getRuntime().availableProcessors()) // default is 50, too low
                //.put("index.number_of_replicas", 0)
                //.put("index.number_of_shards", 1)
                .put("path.home", getHome())
                .build();
    }

    protected String getHome() {
        return "c:\\Users\\miroslaw.piatkowski\\Documents\\Elastic\\bin\\elasticsearch-5.0.1";
        // return System.getProperty("path.home");
    }

    public void startNode(String id) throws IOException, NodeValidationException {
        buildNode(id).start();
    }

    public AbstractClient client(String id) {
        return clients.get(id);
    }

    private void closeNodes() throws IOException {
        logger.info("closing all clients");
        for (AbstractClient client : clients.values()) {
            client.close();
        }
        clients.clear();
        logger.info("closing all nodes");
        for (Node node : nodes.values()) {
            if (node != null) {
                node.close();
            }
        }
        nodes.clear();
        logger.info("all nodes closed");
    }

    protected void findNodeAddress() {
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().transport(true);
        NodesInfoResponse response = client("1").admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
        TransportAddress transportAddress = response.remoteAddress();
        if (transportAddress instanceof InetSocketTransportAddress) {
            InetSocketTransportAddress address = (InetSocketTransportAddress) transportAddress;
            host = address.address().getHostName();
            port = address.address().getPort();
        }
    }

    private Node buildNode(String id) throws IOException {
        Settings nodeSettings = Settings.builder()
                .put(getNodeSettings())
                .put("node.name", id)
                .build();
        logger.info("settings={}", nodeSettings.getAsMap());
        // ES 2.1 renders NodeBuilder as useless
        Node node = new MockNode(nodeSettings, CondBoostPlugin.class);
        AbstractClient client = (AbstractClient)node.client();
        nodes.put(id, node);
        clients.put(id, client);
        logger.info("clients={}", clients);
        return node;
    }

    private static void deleteFiles() throws IOException {
        Path directory = Paths.get(System.getProperty("path.home") + "/data");
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }

        });
    }
}
