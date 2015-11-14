package com.bookmerger;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class CanonicalMapper extends Mapper<Object, Text, Text, BookMapWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        JsonNode result = new ObjectMapper().readTree(value.toString());
        BookMapWritable data = new BookMapWritable();

        JsonNode rawISBN = result.get("isbn");
        Text isbn;
        if (rawISBN != null) {
            String cleanISBN = rawISBN.asText().replace("\"", "");
            isbn = new Text(Utilities.normalizeISBN(cleanISBN));
            data.put(new Text("isbn"), isbn);
        } else {
            // can't map without an isbn
            return;
        }

        JsonNode authorJson = result.get("author_details");
        if (authorJson != null) {
            String authorBlob = authorJson.asText();
            authorBlob  = authorBlob.replace("\"", "'");
            String[] authors = authorBlob.split("\\|");
            String authorString = "[\"" + StringUtils.join(authors, "\",\"") + "\"]";
            data.put(new Text("authors"), new Text(authorString));
        }

        String[] fields = {"title", "publisher", "pages", "list_price", "format", "genre", "date_added"};
        data = Utilities.addByFieldName(fields, result, data);

        if (isbn.getLength() > 0) {
            context.write(isbn, data);
        }
    }
}
