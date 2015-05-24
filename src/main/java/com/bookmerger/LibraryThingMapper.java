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

        // Expect lines of valid JSON
        JsonNode resultJson = new ObjectMapper().readTree(value.toString());
        HashMap<String, Object> result = new ObjectMapper().readValue(value.toString(), HashMap.class);

        BookMapWritable data = new BookMapWritable();

        JsonNode isbns = resultJson.get("isbns");
        if (isbns != null) {
            Iterator<JsonNode> items = isbns.getElements();
            while (items.hasNext()) {
                String item = items.next().asText();
                if (item.length() == 13) {
                    isbn = new Text(item);
                }
            }
        }

        JsonNode tags = resultJson.get("tags");
        Map<String, String> tagCategories = new HashMap<String, String>();
        if (tags != null) {
            Iterator<JsonNode> items = tags.getElements();
            while (items.hasNext()) {
                String type = "TAGS";
                String tag = items.next().asText();

                String[] tagParts = tag.split(":");
                if (tagParts.length > 1) {
                    type = tagParts[0];
                    tag = tagParts[1];
                }
                if (tagCategories.get(type) == null) {
                    tagCategories.put(type, tag);
                } else {
                    tagCategories.put(type, tagCategories.get(type) + "," + tag);
                }
            }
        }
        for (Map.Entry<String, String> entry : tagCategories.entrySet()) {
            data.put(new Text(entry.getKey()), new Text(entry.getValue()));
        }

        for (Map.Entry<String, Object> entry : result.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();

            if (fieldName != null && fieldValue != null) {
                if (fieldName.equals("originalisbn") && isbn == null) {
                    String normalizedISBN = Utilities.normalizeISBN(fieldValue.toString());
                    isbn = new Text(normalizedISBN);
                    data.put(Header.ISBN.asText(), new Text(fieldValue.toString()));
                } else if (fieldName.equals("title")) {
                    data.put(Header.TITLE.asText(), new Text(fieldValue.toString()));
                } else if (fieldName.equals("series")) {
                    data.put(Header.SERIES.asText(), new Text(fieldValue.toString()));
                } else if (fieldName.equals("language")) {
                    data.put(Header.LANGUAGE.asText(), new Text(fieldValue.toString()));
                } else if (fieldName.equals("originallanguage")) {
                    data.put(Header.ORIGINAL_LANGUAGE.asText(), new Text(fieldValue.toString()));
                } else if (fieldName.equals("fromwhere")) {
                    data.put(Header.FROM_WHERE.asText(), new Text(fieldValue.toString()));
                }
            }
        }
        if (isbn != null) {
            context.write(isbn, data);
        }
    }
}
