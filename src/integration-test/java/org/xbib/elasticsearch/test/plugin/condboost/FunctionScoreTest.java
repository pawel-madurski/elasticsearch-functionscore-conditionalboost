package org.xbib.elasticsearch.test.plugin.condboost;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;

import java.util.Arrays;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.weightFactorFunction;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;

public class FunctionScoreTest {

    public void testFunctionScore() {
        QueryBuilder query = boolQuery()
                .must(matchQuery("party_id", "12"))
                .must(termsQuery("course_cd", Arrays.asList("writ100", "writ112", "writ113")));

        SearchRequest searchRequest = searchRequest()
                .source(
                        searchSource().query(
                                new FunctionScoreQueryBuilder(query, weightFactorFunction(3.0f))
                        )
                );
    }
}
