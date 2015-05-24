package com.bookmerger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class LTScrapedMapper extends Mapper<Object, Text, Text, BookMapWritable> {
    private Text isbn = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Expect lines of valid JSON
        JsonNode result = new ObjectMapper().readTree(value.toString());
        BookMapWritable data = new BookMapWritable();

        JsonNode isbn10 = result.get("isbn");
        isbn = new Text(Utilities.normalizeISBN(isbn10.asText()));

        String[] fields = {"places", "characters", "events", "year"};

        for (String field : fields) {
            Header header;
            try {
                header = Header.valueOf(field.toUpperCase());
            } catch (IllegalArgumentException e) {
                continue;
            }
            JsonNode fieldValue = result.get(field);

            if (fieldValue != null) {
                data.put(header.asText(), new Text(fieldValue.toString()));
            }
        }

        if (isbn != null) {
            context.write(isbn, data);
        }
    }
}
