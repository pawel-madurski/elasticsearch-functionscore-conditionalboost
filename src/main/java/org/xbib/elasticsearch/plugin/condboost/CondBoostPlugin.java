package org.xbib.elasticsearch.plugin.condboost;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.xbib.elasticsearch.index.query.functionscore.condboost.CondBoostFactorFunctionBuilder;
import org.xbib.elasticsearch.index.query.functionscore.condboost.CondBoostFactorFunctionParser;

import java.util.Collections;
import java.util.List;

public class CondBoostPlugin extends Plugin implements SearchPlugin {
    @Override
    public List<ScoreFunctionSpec<?>> getScoreFunctions() {
        return Collections.singletonList(new ScoreFunctionSpec<>(
                CondBoostFactorFunctionParser.getNAMES()[0],
                CondBoostFactorFunctionBuilder::new,
                CondBoostFactorFunctionBuilder.PARSER
        ));
    }
}

