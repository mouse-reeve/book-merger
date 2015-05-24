package com.bookmerger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CanonicalMapper extends Mapper<Object, Text, Text, BookMapWritable> {
    private Text isbn = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Expect lines of valid JSON
        HashMap<String, Object> result = new ObjectMapper().readValue(value.toString(), HashMap.class);

        BookMapWritable data = new BookMapWritable();
        for (Map.Entry<String, Object> entry : result.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();

            if (fieldName != null && fieldValue != null) {
                if (fieldName.equals("isbn")) {
                    String normalizedISBN = Utilities.normalizeISBN(fieldValue.toString());
                    isbn = new Text(normalizedISBN);
                    data.put(Header.ISBN.asText(), new Text(fieldValue.toString()));
                } else if (fieldName.equals("author_details")) {
                    // TODO: why does the array break everything?
                    //ArrayWritable authors = new ArrayWritable(fieldValue.toString().split("|"));
                    data.put(Header.AUTHORS.asText(), new Text(fieldValue.toString()));
                } else if (fieldName.equals("title")) {
                    data.put(Header.TITLE.asText(), new Text(fieldValue.toString()));
                } else if (fieldName.equals("publisher")) {
                    data.put(Header.PUBLISHER.asText(), new Text(fieldValue.toString()));
                } else if (fieldName.equals("series_details")) {
                    data.put(Header.SERIES.asText(), new Text(fieldValue.toString()));
                } else if (fieldName.equals("pages")) {
                    data.put(Header.PAGES.asText(), new Text(fieldValue.toString()));
                } else if (fieldName.equals("list_price")) {
                    data.put(Header.LIST_PRICE.asText(), new Text(fieldValue.toString()));
                } else if (fieldName.equals("format")) {
                    data.put(Header.FORMAT.asText(), new Text(fieldValue.toString()));
                } else if (fieldName.equals("genre")) {
                    data.put(Header.GENRE.asText(), new Text(fieldValue.toString()));
                } else if (fieldName.equals("date_added")) {
                    data.put(Header.DATE_ADDED.asText(), new Text(fieldValue.toString()));
                }
            }
        }
        if (isbn.getLength() > 0) {
            context.write(isbn, data);
        }
    }
}
