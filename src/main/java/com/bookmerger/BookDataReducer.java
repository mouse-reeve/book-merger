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
                data.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }

        context.write(key, data);
    }
}
