package com.bookmerger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class LTScrapedMapper extends Mapper<Object, Text, Text, BookMapWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        JsonNode result = new ObjectMapper().readTree(value.toString());
        BookMapWritable data = new BookMapWritable();

        JsonNode isbn10 = result.get("isbn");
        Text isbn = new Text(Utilities.normalizeISBN(isbn10.asText()));

        String[] fields = {"places", "characters", "events", "date_first_published"};
        data = Utilities.addByFieldName(fields, result, data);

        context.write(isbn, data);
    }
}
