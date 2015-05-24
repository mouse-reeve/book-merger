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

        JsonNode places = result.get("places");
        if (places != null) {
            data.put(Header.PLACES.asText(), new Text(places.toString()));
        }

        JsonNode characters = result.get("characters");
        if (characters != null) {
        data.put(Header.CHARACTERS.asText(), new Text(characters.toString()));
        }

        JsonNode events = result.get("events");
        if (events != null) {
            data.put(Header.EVENTS.asText(), new Text(events.toString()));
        }

        JsonNode year = result.get("year");
        if (year != null) {
            data.put(Header.DATE_PUBLISHED.asText(), new Text(year.toString()));
        }

        if (isbn != null) {
            context.write(isbn, data);
        }
    }
}
