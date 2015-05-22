package com.bookmerger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BookDataMapper extends Mapper<Object, Text, Text, BookMapWritable> {
    private String[] headers;
    private Text isbn = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // TODO: this is a terrible way to handle different CSV formats
        String[] values = value.toString().split("\",\"");
        if (values.length == 1) {
            values = value.toString().split(",");
        }
        BookMapWritable data = new BookMapWritable();

        // TODO: there MUST be a less dumb way of doing this
        if (key.toString().equals("0")) {
            headers = values;
        } else {
            for (int i = 0; i < values.length; i++) {
                String header = headers[i];
                String datum = values[i];
                if (header.equals("isbn") || header.equals("isbn13")) {
                    isbn.set(datum);
                } else {
                    Header h = Header.findHeader(header);
                    if (h != null) {
                        Text csvKey = new Text(h.getName());
                        Text csvValue = new Text(datum);
                        data.put(csvKey, csvValue);
                    }
                }
            }
            context.write(isbn, data);
        }
    }
}
