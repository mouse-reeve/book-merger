package com.bookmerger;

import org.apache.hadoop.io.Text;
import org.codehaus.jackson.JsonNode;

public class Utilities {
    public static String normalizeISBN(String isbn) {
        if (isbn.length() == 10) {
            try {
                Integer.parseInt(isbn);
            } catch (NumberFormatException e) {
                return isbn;
            }
            return isbn10toISBN13(isbn);
        }
        return isbn;
    }

    private static String isbn10toISBN13(String isbn10) {
        String isbn13 = "978";
        isbn10 = isbn10.substring(0, 9);

        int sum = 0;
        for (int i = 0; i < isbn10.length(); i++) {
            int digit = Integer.parseInt(isbn10.substring(i, i+1));
            if (i % 2 == 0) {
                digit *= 3;
            }
            sum += digit;
        }

        int isbn13CheckDigit = sum % 10;

        isbn13 += isbn10 + isbn13CheckDigit;
        return isbn13;
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
