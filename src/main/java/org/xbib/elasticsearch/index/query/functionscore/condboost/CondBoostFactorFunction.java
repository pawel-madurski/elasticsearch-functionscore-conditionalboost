package org.xbib.elasticsearch.index.query.functionscore.condboost;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.LeafScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class CondBoostFactorFunction extends ScoreFunction {

    private final QueryShardContext queryShardContext;

    // TODO: use list?
    private final CondBoostEntry condBoostEntry;

    private final float boostFactor;

    private final Modifier modifier;

    private final float defaultBoost;

    private float boost;

    CondBoostFactorFunction(QueryShardContext queryShardContext, CondBoostEntry condBoostEntry,
                            float defaultBoost, float boostFactor, Modifier modifierType) {
        super(CombineFunction.MULTIPLY);
        this.queryShardContext = queryShardContext;
        this.condBoostEntry = condBoostEntry;
        this.defaultBoost = defaultBoost;
        this.boostFactor = boostFactor;
        this.modifier = modifierType;
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(final LeafReaderContext ctx) throws IOException {
        return new LeafScoreFunction() {
            @Override
            public double score(int docId, float subQueryScore) {
                boost = defaultBoost;
                String currentFieldName = condBoostEntry.fieldName;
                MappedFieldType mappedFieldType = queryShardContext.getMapperService().fullName(currentFieldName);
                if (mappedFieldType == null) {
                    throw new ElasticsearchException("unable to find field [" + currentFieldName + "]");
                }
                IndexFieldData indexFieldData = queryShardContext.getForField(mappedFieldType);
                SortedBinaryDocValues values = indexFieldData.load(ctx).getBytesValues();
                values.setDocument(docId);
                for (int i = 0; i < values.count(); i++) {
                    if (condBoostEntry.fieldValueList.contains(values.valueAt(i).utf8ToString())) {
                        boost = boost * condBoostEntry.boost; // multiply boosts by default
                    }
                }
                double val = boost * boostFactor;
                double result = modifier.apply(val);
                if (Double.isNaN(result) || Double.isInfinite(result)) {
                    throw new ElasticsearchException("result of field modification [" + modifier.toString() +
                            "(" + val + ")] must be a number");
                }
                return result;
            }

            @Override
            public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
                return Explanation.match(boost, "condboost");
            }
        };
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    @Override
    protected boolean doEquals(ScoreFunction other) {
        CondBoostFactorFunction o = (CondBoostFactorFunction)other;
        return this.boostFactor == o.boostFactor &&
                this.modifier == o.modifier &&
                this.defaultBoost == o.defaultBoost &&
                Objects.equals(this.condBoostEntry, o.condBoostEntry);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.boostFactor, this.modifier, this.defaultBoost, this.condBoostEntry);
    }

    @Override
    public String toString() {
        return "condboost[" + boost + "]";
    }

    public enum Modifier implements Writeable {
        NONE {
            @Override
            public double apply(double n) {
                return n;
            }
        },
        LOG {
            @Override
            public double apply(double n) {
                return Math.log10(n);
            }
        },
        LOG1P {
            @Override
            public double apply(double n) {
                return Math.log10(n + 1);
            }
        },
        LOG2P {
            @Override
            public double apply(double n) {
                return Math.log10(n + 2);
            }
        },
        LN {
            @Override
            public double apply(double n) {
                return Math.log(n);
            }
        },
        LN1P {
            @Override
            public double apply(double n) {
                return Math.log1p(n);
            }
        },
        LN2P {
            @Override
            public double apply(double n) {
                return Math.log1p(n + 1);
            }
        },
        SQUARE {
            @Override
            public double apply(double n) {
                return Math.pow(n, 2);
            }
        },
        SQRT {
            @Override
            public double apply(double n) {
                return Math.sqrt(n);
            }
        },
        RECIPROCAL {
            @Override
            public double apply(double n) {
                return 1.0 / n;
            }
        };

        public abstract double apply(double n);

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.ordinal());
        }

        public static Modifier readFromStream(StreamInput in) throws IOException {
            int ordinal = in.readVInt();
            if (ordinal < 0 || ordinal >= values().length) {
                throw new IOException("Unknown Modifier ordinal [" + ordinal + "]");
            }
            return values()[ordinal];
        }

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }

        public static Modifier fromString(String modifier) {
            return valueOf(modifier.toUpperCase(Locale.ROOT));
        }
    }
}
