package org.xbib.elasticsearch.test.plugin.condboost;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;
import org.xbib.elasticsearch.index.query.functionscore.condboost.CondBoostFactorFunction;
import org.xbib.elasticsearch.index.query.functionscore.condboost.CondBoostFactorFunctionBuilder;
import org.xbib.elasticsearch.test.NodeTestUtils;

import java.io.IOException;
import java.util.HashSet;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.junit.Assert.assertEquals;

public class FunctionScorePluginTest extends NodeTestUtils {

    private static final String docType = "products";

    @Test
    public void testSearch() throws Exception {
        IndicesAdminClient adminClient = client("1").admin().indices();
        prepareIndex(adminClient);

        client("1").admin().cluster().prepareHealth()
                .setWaitForYellowStatus().execute().actionGet();

        IndexProducts();

        // wait for refresh
        adminClient.prepareRefresh(indexName).get();

        HashSet<String> condBoostFieldValues3 = new HashSet<>();
        condBoostFieldValues3.add("product_name_3");

        CondBoostFactorFunctionBuilder cbfb3 = new CondBoostFactorFunctionBuilder()
                .factor(1.0f)
                .modifier(CondBoostFactorFunction.Modifier.NONE)
                .condBoost("product", condBoostFieldValues3, 4.0f);

        HashSet<String> condBoostFieldValues2 = new HashSet<>();
        condBoostFieldValues2.add("product_name_2");

        CondBoostFactorFunctionBuilder cbfb2 = new CondBoostFactorFunctionBuilder()
                .factor(1.0f)
                .modifier(CondBoostFactorFunction.Modifier.NONE)
                .condBoost("product", condBoostFieldValues2, 2.0f);

        HashSet<String> condBoostFieldValues = new HashSet<>();
        condBoostFieldValues.add("product_name_1");
        condBoostFieldValues.add("product_name_3");

        CondBoostFactorFunctionBuilder cbfb = new CondBoostFactorFunctionBuilder()
                .factor(1.0f)
                .modifier(CondBoostFactorFunction.Modifier.NONE)
                .condBoost("product", condBoostFieldValues, 5.0f);

        // TODO: fix test
//        SearchRequest searchRequest = searchRequest()
//                .source(searchSource()
//                        .explain(true)
//                        .query(functionScoreQuery(matchAllQuery(), cbfb3).add(cbfb3).add(cbfb2).add(cbfb)));
//
//        SearchResponse sr = client("1").search(searchRequest).actionGet();
//        SearchHits sh = sr.getHits();
//
//        //for (int i = 0; i < sh.hits().length; i++) {
//        //     System.err.println( sh.getAt(i).getId() + " " + sh.getAt(i).getScore() + " -->" + sh.getAt(i).getSource());
//        //}
//
//        assertEquals(sh.hits().length, 3);
//
//        assertEquals("3", sh.getAt(0).getId());
//        assertEquals("1", sh.getAt(1).getId());
//        assertEquals("2", sh.getAt(2).getId());

    }

    private void IndexProducts() throws IOException {
        client("1").prepareIndex(indexName, docType, "1")
                .setSource(jsonBuilder().startObject()
                        .field("content", "foo bar")
                        .field("product", "product_name_1")
                        .endObject()
                ).get();

        client("1").prepareIndex(indexName, docType, "2")
                .setSource(jsonBuilder().startObject()
                        .field("content", "foo bar")
                        .field("product", "product_name_2")
                        .endObject()
                ).get();

        client("1").prepareIndex(indexName, docType, "3")
                .setSource(jsonBuilder().startObject()
                        .field("content", "foo bar")
                        .field("product", "product_name_3")
                        .endObject()
                ).get();
    }

    private void prepareIndex(IndicesAdminClient adminClient) {
        if (adminClient.prepareExists(indexName).execute().actionGet().isExists()) {
            adminClient.prepareDelete(indexName).execute().actionGet();
        }

        Settings indexSettings = Settings.builder()
                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .build();

        adminClient
                .prepareCreate(indexName)
                .setSettings(indexSettings)
                .addMapping(docType, "{\n" +
                        "  \"products\": {\n" +
                        "    \"properties\": {\n" +
                        "      \"content\": {\n" +
                        "        \"type\": \"text\"\n" +
                        "      },\n" +
                        "      \"product\": {\n" +
                        "        \"type\": \"keyword\"\n" +
                        "      },\n" +
                        "      \"user\": {\n" +
                        "        \"type\": \"keyword\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}")
                .get();
    }
}
