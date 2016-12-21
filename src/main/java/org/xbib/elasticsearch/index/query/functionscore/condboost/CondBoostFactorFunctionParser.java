
package org.xbib.elasticsearch.index.query.functionscore.condboost;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.functionscore.ScoreFunctionParser;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;

/**
 * Parses out a function_score function that looks like:
 *
 * <pre>
 *     {
 *         "cond_boost": {
 *             "value" : 1.0,
 *             "factor" : 1.0,
 *             "modifier" : "NONE",
 *             "cond" :  {
 *                 "fieldName" : "product",
 *                 "fieldValues" : ["product_name_1"],
 *                 "value" : 5.0
 *              }
 *         }
 *     }
 * </pre>
 */
public class CondBoostFactorFunctionParser implements ScoreFunctionParser<CondBoostFactorFunctionBuilder> {

    static String[] NAMES = { "cond_boost", "condBoost" };

    public static String[] getNAMES() {
        return NAMES;
    }

    @Override
    public CondBoostFactorFunctionBuilder fromXContent(QueryParseContext context) throws IOException, ParsingException {
        XContentParser parser = context.parser();

        String currentFieldName = null;
        CondBoostEntry condBoost = new CondBoostEntry();
        float defaultBoost = 1.0f;
        float boostFactor = 1.0f;
        CondBoostFactorFunction.Modifier modifier = CondBoostFactorFunction.Modifier.NONE;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            }else {
                if (currentFieldName != null) {
                    switch (currentFieldName) {
                        case CondBoostEntry.BOOST:
                            defaultBoost = parser.floatValue();
                            break;
                        case "factor":
                            boostFactor = parser.floatValue();
                            break;
                        case "modifier":
                            modifier = CondBoostFactorFunction.Modifier.valueOf(parser.text().toUpperCase(Locale.ROOT));
                            break;
                        case "cond":
                            condBoost = parseCond(parser, currentFieldName);
                            break;
                        default:
                            throw new ParsingException(parser.getTokenLocation(), NAMES[0] + " query does not support [" + currentFieldName + "]");
                    }
                }
            }
        }

        return new CondBoostFactorFunctionBuilder(defaultBoost, boostFactor, modifier, condBoost);
    }

    private CondBoostEntry parseCond(XContentParser parser, String currentFieldName) throws IOException {
        XContentParser.Token token;
        CondBoostEntry entry = new CondBoostEntry();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else {
                switch(currentFieldName){
                    case (CondBoostEntry.BOOST):
                        entry.boost = parser.floatValue();
                        break;
                    case ("fieldName"):
                        entry.fieldName = parser.text();
                        break;
                    case ("fieldValues"):
                        HashSet<String> fieldValueList = new HashSet<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token.isValue()){
                                String fieldValue = parser.text();
                                fieldValueList.add(fieldValue);
                            }
                        }
                        entry.fieldValueList = fieldValueList;
                        break;
                }

            }
        }

        return entry;
    }

//    private List<CondBoostEntry> parseCondArray(XContentParser parser, String currentFieldName) throws IOException {
//        XContentParser.Token token;
//        List<CondBoostEntry> condArray = new LinkedList<>();
//        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
//            if (token != XContentParser.Token.START_OBJECT) {
//                throw new ParsingException(parser.getTokenLocation(), "malformed query, expected a "
//                        + XContentParser.Token.START_OBJECT + " while parsing cond boost array, but got a " + token);
//            } else {
//                CondBoostEntry entry = new CondBoostEntry();
//                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
//                    if (token == XContentParser.Token.FIELD_NAME) {
//                        currentFieldName = parser.currentName();
//                    } else {
//                        if (CondBoostEntry.BOOST.equals(currentFieldName)) {
//                            entry.boost = parser.floatValue();
//                        } else {
//                            entry.fieldName = currentFieldName;
//                            entry.fieldValue = parser.text();
//                        }
//                    }
//                }
//                condArray.add(entry);
//            }
//        }
//        return condArray;
//    }
}
