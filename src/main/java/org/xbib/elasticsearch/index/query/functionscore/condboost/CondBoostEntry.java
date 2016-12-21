package org.xbib.elasticsearch.index.query.functionscore.condboost;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;

class CondBoostEntry implements ToXContent, Comparable<CondBoostEntry>, Writeable {
    final static String BOOST = "value";

    String fieldName;

    HashSet<String> fieldValueList;

    float boost;

    // region Constructors

    CondBoostEntry() {
    }

    CondBoostEntry(StreamInput in) throws IOException {
        this.fieldName = in.readOptionalString();
        String[] array = in.readOptionalStringArray();
        if (array != null) {
            this.fieldValueList = new HashSet<>(Arrays.asList(array));
        }
        this.boost = in.readOptionalFloat();
    }

    // endregion

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("cond");
        builder.field("fieldName", fieldName);

        if (fieldValueList != null) {
            builder.startArray("fieldValues");
            for (String entry : fieldValueList) {
                builder.value(entry);
            }
            builder.endArray();
        }
        builder.field(BOOST, boost);
        builder.endObject();
        return builder;
    }

    public String toString() {
        String fieldValuesJoined = String.join(",", this.fieldValueList);
        return "{fieldName = " + fieldName + ", fieldValues = [" + fieldValuesJoined + " ], " + BOOST + "=" + boost + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CondBoostEntry)) {
            return false; // ------------------>
        }
        CondBoostEntry other = (CondBoostEntry) obj;
        return Objects.equals(this.fieldName, other.fieldName) &&
                this.boost == other.boost &&
                ListHelper.equalLists(this.fieldValueList, other.fieldValueList);
    }

    @Override
    public int compareTo(CondBoostEntry o) {
        if (this.equals(o)) {
            return 0; // ------------->
        }
        int result = this.fieldName.compareTo(o.fieldName);
        if (result == 0) {
            result = Float.compare(this.boost, o.boost);

            if (result == 0) {
                String thisJoin = "";
                String otherJoin = "";
                if (this.fieldValueList != null) {
                    thisJoin = String.join(",", this.fieldValueList);
                }
                if (o.fieldValueList != null) {
                    otherJoin = String.join(",", o.fieldValueList);
                }
                result = thisJoin.compareTo(otherJoin);
            }
        }
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(this.fieldName);
        String[] array = null;
        if (this.fieldValueList != null) {
            array = this.fieldValueList.toArray(new String[this.fieldValueList.size()]);
        }
        out.writeOptionalStringArray(array);
        out.writeOptionalFloat(this.boost);
    }

    static CondBoostEntry fromStream(StreamInput in) throws IOException {
        CondBoostEntry result = new CondBoostEntry(in);
        return result;
    }
}