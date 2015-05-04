import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BookMerger {

    static final String[] authorSynonyms = {"auth", "author"};
    static final String[] titleSynonyms = {"title"};
    public enum Header {
        AUTHOR ("author", authorSynonyms),
        LIST_PRICE("list_price", null),
        PAGES ("pages", null),
        PUBLISHER ("publisher", null),
        TITLE ("title", titleSynonyms);

        private final String name;
        private final String[] synonyms;
        Header(String name, String[] synonyms) {
            this.name = name;
            this.synonyms = synonyms;
        }

        private String[] synonyms() {
            return synonyms;
        }

        public static Header getHeader(String key) {
            for (Header h : Header.values()) {
                if (key.equals(h.name)) {
                    return h;
                }
            }
            return null;
        }
    }

    public static class BookMapWritable extends MapWritable {
        @Override public String toString() {
            String result = "{";
            for (Map.Entry<Writable, Writable> entry : this.entrySet()) {
                result += "\"" + entry.getKey() + "\":\"" + entry.getValue() + "\",";
            }
            return result + "}";
        }
    }

    public static class BookDataMapper extends Mapper<Object, Text, Text, BookMapWritable> {

        private String[] headers;
        private Text isbn = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\",\"");
            BookMapWritable data = new BookMapWritable();

            // TODO: there MUST be a less dumb way of doing this
            if (key.toString().equals("0")) {
                headers = values;
            } else {
                for (int i = 0; i < values.length; i++) {
                    String header = headers[i];
                    String datum = values[i];
                    if (header.equals("isbn")) {
                        isbn.set(datum);
                    } else {
                        Header h = Header.getHeader(header);
                        if (h != null) {
                            Text csvKey = new Text(h.name);
                            Text csvValue = new Text(datum);
                            data.put(csvKey, csvValue);
                        }
                    }
                }
                context.write(isbn, data);
            }
        }
    }

    public static class BookDataReducer extends Reducer<Text, BookMapWritable, Text, BookMapWritable> {

        public void reduce(Text key, Iterable<BookMapWritable> values, Context context) throws IOException, InterruptedException {
            BookMapWritable data = new BookMapWritable();
            for (MapWritable dataMap : values) {
                for (Map.Entry<Writable, Writable> entry : dataMap.entrySet()) {
                    Writable existingEntry = data.get(entry.getKey());
                    if (existingEntry != null && !existingEntry.equals(entry.getValue())) {
                        // Mismatched values for the same header
                        Text newEntry = new Text(existingEntry + "||" + entry.getValue());
                        data.put(entry.getKey(), newEntry);
                    } else {
                        data.put(entry.getKey(), entry.getValue());
                    }
                }
            }

            context.write(key, data);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "book merger");

        job.setJarByClass(BookMerger.class);
        job.setMapperClass(BookDataMapper.class);
        job.setCombinerClass(BookDataReducer.class);
        job.setReducerClass(BookDataReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BookMapWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
