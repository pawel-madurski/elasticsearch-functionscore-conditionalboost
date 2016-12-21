package org.xbib.elasticsearch.index.query.functionscore.condboost;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Used to read lists from stream.
 *
 * Created by miroslaw.piatkowski on 12/21/2016.
 */
public class CondBoostEntryReader implements Writeable.Reader<CondBoostEntry> {
    @Override
    public CondBoostEntry read(StreamInput in) throws IOException {
        return new CondBoostEntry(in);
    }
}
