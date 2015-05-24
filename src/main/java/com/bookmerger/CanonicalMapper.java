package com.bookmerger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class CanonicalMapper extends Mapper<Object, Text, Text, BookMapWritable> {
    private Text isbn = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        JsonNode result = new ObjectMapper().readTree(value.toString());
        BookMapWritable data = new BookMapWritable();

        JsonNode rawISBN = result.get("isbn");
        if (rawISBN != null) {
            isbn = new Text(Utilities.normalizeISBN(rawISBN.asText()));
            data.put(new Text("isbn"), new Text(rawISBN.toString()));
        }

        String[] fields = {"title", "author_details", "publisher", "pages", "list_price", "format", "genre", "date_added"};
        data = Utilities.addByFieldName(fields, result, data);

        if (isbn.getLength() > 0) {
            context.write(isbn, data);
        }
    }
}
