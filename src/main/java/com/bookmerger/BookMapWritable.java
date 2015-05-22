package com.bookmerger;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.util.Map;

public class BookMapWritable extends MapWritable {
    @Override public String toString() {
        String result = "{";
        for (Map.Entry<Writable, Writable> entry : this.entrySet()) {
            result += "\"" + entry.getKey() + "\":\"" + entry.getValue() + "\",";
        }
        return result + "}";
    }
}
