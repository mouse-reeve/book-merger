package com.bookmerger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class LibraryThingMapper extends Mapper<Object, Text, Text, BookMapWritable> {
    private Text isbn = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        isbn = null;
        JsonNode result = new ObjectMapper().readTree(value.toString());

        BookMapWritable data = new BookMapWritable();

        JsonNode isbns = result.get("isbns");
        if (isbns != null) {
            Iterator<JsonNode> items = isbns.getElements();
            while (items.hasNext()) {
                String item = items.next().asText();
                if (item.length() == 13) {
                    isbn = new Text(item);
                }
            }
        }

        JsonNode tags = result.get("tags");
        Map<String, String> tagCategories = new HashMap<String, String>();
        if (tags != null) {
            Iterator<JsonNode> items = tags.getElements();
            while (items.hasNext()) {
                String type = "tags";
                String tag = items.next().asText();

                String[] tagParts = tag.split(":");
                if (tagParts.length > 1) {
                    type = tagParts[0].toLowerCase();
                    tag = tagParts[1];
                }
                if (tagCategories.get(type) == null) {
                    tagCategories.put(type, "\"" + tag + "\"");
                } else {
                    tagCategories.put(type, tagCategories.get(type) + "," + "\"" + tag + "\"");
                }
            }
        }
        for (Map.Entry<String, String> entry : tagCategories.entrySet()) {
            data.put(new Text(entry.getKey()), new Text("[" + entry.getValue() + "]"));
        }

        String[] fields = {"series", "language", "originallanguage", "fromwhere"};
        data = Utilities.addByFieldName(fields, result, data);

        JsonNode originalISBN = result.get("originalisbn");
        if (originalISBN != null && isbn == null) {
            String normalizedISBN = Utilities.normalizeISBN(originalISBN.toString());
            isbn = new Text(normalizedISBN);
            data.put(new Text("isbn"), new Text(originalISBN.toString()));
        }
        if (isbn != null) {
            context.write(isbn, data);
        }
    }
}
