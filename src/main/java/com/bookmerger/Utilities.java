package com.bookmerger;

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
        int isbn10CheckDigit = Integer.parseInt(isbn10.substring(isbn10.length()-1));
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
}
