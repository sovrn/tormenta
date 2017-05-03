package com.twitter.tormenta.spout;

import java.io.Serializable;
import java.util.List;

import org.apache.storm.utils.Utils;

/**
 * cribbed from the apache storm project, as its not present in the heron cut of it.
 * @author jpowers
 *
 */
public class FixedTuple implements Serializable {
    public String stream;
    public List<Object> values;

    public FixedTuple(List<Object> values) {
        this.stream = Utils.DEFAULT_STREAM_ID;
        this.values = values;
    }

    public FixedTuple(String stream, List<Object> values) {
        this.stream = stream;
        this.values = values;
    }

    @Override
    public String toString() {
        return stream + ":" + "<" + values.toString() + ">";
    }
}
