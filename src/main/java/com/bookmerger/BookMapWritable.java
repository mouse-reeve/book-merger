package com.bookmerger;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.Map;

/**
 * Produces a usable output from the the MapWritable toString method
 */
public class BookMapWritable extends MapWritable {
    @Override public String toString() {
        String result = "{";

        String[] entries = new String[this.size()];
        int i = 0;
        for (Map.Entry<Writable, Writable> entry : this.entrySet()) {
            String value = entry.getValue().toString();
            if (value.charAt(0) != '[') {
                value = "\"" + value + "\"";
            }
            entries[i] = "\"" + entry.getKey() + "\":" + value;
            i++;
        }
        result += StringUtils.join(entries, ',');
        return result + "}";
    }
}
