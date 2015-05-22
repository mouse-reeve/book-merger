package com.bookmerger;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

public class BookDataReducer extends Reducer<Text, BookMapWritable, Text, BookMapWritable> {

    public void reduce(Text key, Iterable<BookMapWritable> values, Context context) throws IOException, InterruptedException {
        BookMapWritable data = new BookMapWritable();
        for (MapWritable dataMap : values) {
            for (Map.Entry<Writable, Writable> entry : dataMap.entrySet()) {
                Writable existingEntry = data.get(entry.getKey());
                if (existingEntry != null && !existingEntry.equals(entry.getValue())) {
                    // Mismatched values for the same header
                    Text newEntry = new Text(existingEntry + "||" + entry.getValue());
                    data.put(entry.getKey(), newEntry);
                } else {
                    data.put(entry.getKey(), entry.getValue());
                }
            }
        }

        context.write(key, data);
    }
}
