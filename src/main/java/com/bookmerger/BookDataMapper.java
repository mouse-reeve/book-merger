package com.bookmerger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BookDataMapper extends Mapper<Object, Text, Text, BookMapWritable> {
    private Text isbn = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Expect lines of valid JSON
        HashMap<String, Object> result = new ObjectMapper().readValue(value.toString(), HashMap.class);

        BookMapWritable data = new BookMapWritable();
        for (Map.Entry<String, Object> entry : result.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();

            Header header = Header.findHeader(fieldName);

            if (header != null && fieldValue != null) {
                if (header.equals(Header.ISBN)) {
                    isbn = new Text(fieldValue.toString());
                }

                data.put(new Text(header.getName()), new Text(fieldValue.toString()));
            }
        }
        context.write(isbn, data);
    }
}
