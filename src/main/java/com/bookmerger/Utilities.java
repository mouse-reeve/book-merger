package com.bookmerger;

import org.apache.hadoop.io.Text;
import org.codehaus.jackson.JsonNode;

public class Utilities {
    public static String normalizeISBN(String isbn) {
        if (isbn.length() == 10) {
            try {
                Integer.parseInt(isbn.substring(0, 9));
            } catch (NumberFormatException e) {
                return isbn;
            }
            return isbn10toISBN13(isbn);
        }
        return isbn;
    }

    private static String isbn10toISBN13(String isbn10) {
        //isbn13 = 10 - (x_1 + 3x_2 + x_3 + 3x_4 + ... + x_11 + 3x_12) mod10 ) mod 10
        String isbn13 = "978" + isbn10.substring(0, 9);

        int sum = 0;
        for (int i = 0; i < isbn13.length(); i++) {
            int digit = Integer.parseInt(isbn13.substring(i, i+1));
            if (i % 2 == 1) {
                digit *= 3;
            }
            sum += digit;
        }

        int isbn13CheckDigit = (10 - (sum % 10)) % 10;

        return isbn13 + isbn13CheckDigit;
    }

    public static BookMapWritable addByFieldName(String[] fields, JsonNode input, BookMapWritable data) {
        for (String field : fields) {
            JsonNode fieldValue = input.get(field);

            if (fieldValue != null) {
                data.put(new Text(field), new Text(fieldValue.toString()));
            }
        }

        return data;
    }
}
