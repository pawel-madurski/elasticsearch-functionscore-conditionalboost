package org.xbib.elasticsearch.index.query.functionscore.condboost;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;

import java.io.IOException;
import java.util.*;

public class CondBoostFactorFunctionBuilder extends ScoreFunctionBuilder<CondBoostFactorFunctionBuilder> {

    public static CondBoostFactorFunctionParser PARSER = new CondBoostFactorFunctionParser();

    private Float value = 1.0f;

    private Float factor = 1.0f;

    private CondBoostFactorFunction.Modifier modifier = CondBoostFactorFunction.Modifier.NONE;

    // TODO: use list?
    private CondBoostEntry entry = new CondBoostEntry();

    // region Constructors

    CondBoostFactorFunctionBuilder(float value, float factor, CondBoostFactorFunction.Modifier modifier,
                                   CondBoostEntry entry){
        super();
        this.value = value;
        this.factor = factor;
        this.modifier = modifier;
        this.entry = entry;
    }

    public CondBoostFactorFunctionBuilder() {
        super();
    }

    public CondBoostFactorFunctionBuilder(StreamInput in) throws IOException {
        super(in);

        // optional entry
        if (in.readBoolean()) {
            this.entry = CondBoostEntry.fromStream(in);
        }

        this.factor = in.readOptionalFloat();
        this.value = in.readOptionalFloat();

        // optional modifier
        if (in.readBoolean()) {
            this.modifier = CondBoostFactorFunction.Modifier.readFromStream(in);
        }
    }

    // endregion

    // region ScoreFunctionBuilder members

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        // optional entry list
        if (this.entry == null) {
            out.writeBoolean(false);
        }
        else {
            out.writeBoolean(true);
            this.entry.writeTo(out);
        }

        out.writeOptionalFloat(this.factor);
        out.writeOptionalFloat(this.value);

        // optional modifier
        if (this.modifier == null) {
            out.writeBoolean(false);
        }
        else {
            out.writeBoolean(true);
            this.modifier.writeTo(out);
        }
    }

    @Override
    public String getName() {
        return CondBoostFactorFunctionParser.NAMES[0];
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());

        entry.toXContent(builder, params);

        if (value != null) {
            builder.field("value", value);
        }

        if (factor != null) {
            builder.field("factor", factor);
        }

        if (modifier != null) {
            builder.field("modifier", modifier.toString().toLowerCase(Locale.ROOT));
        }

        builder.endObject();
    }

    @Override
    protected boolean doEquals(CondBoostFactorFunctionBuilder functionBuilder) {
        return this.value == functionBuilder.value &&
                this.factor == functionBuilder.factor &&
                this.modifier == functionBuilder.modifier &&
                Objects.equals(this.entry, functionBuilder.entry);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.value, this.factor, this.modifier, this.entry);
    }

    @Override
    protected ScoreFunction doToFunction(QueryShardContext context) throws IOException {
        CondBoostFactorFunction result = new CondBoostFactorFunction(context, this.entry, this.value, this.factor, this.modifier);
        return result;
    }

    // endregion

    public CondBoostFactorFunctionBuilder condBoost(String fieldName,
                                                    HashSet<String> fieldValueList, float boost) {
        entry = new CondBoostEntry();
        entry.fieldName = fieldName;
        entry.fieldValueList = fieldValueList;
        entry.boost = boost;
        return this;
    }

    public CondBoostFactorFunctionBuilder value(float boost) {
        this.value = boost;
        return this;
    }

    public CondBoostFactorFunctionBuilder factor(float boostFactor) {
        this.factor = boostFactor;
        return this;
    }

    public CondBoostFactorFunctionBuilder modifier(CondBoostFactorFunction.Modifier modifier) {
        this.modifier = modifier;
        return this;
    }
}
