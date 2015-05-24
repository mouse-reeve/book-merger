package com.bookmerger;

import org.apache.hadoop.io.Text;

public enum Header {
    AUTHORS,
    FORMAT,
    GENRE,
    LIST_PRICE,
    PAGES,
    PUBLISHER,
    READ_START,
    READ_FINISH,
    DATE_ADDED,
    SERIES,
    TITLE,
    TAGS,
    RECOMMENDER,
    TYPE,
    MOOD,
    READABILITY,
    LANGUAGE,
    ORIGINAL_LANGUAGE,
    FROM_WHERE,
    ISBN;

    public Text asText() {
        return new Text(this.toString());
    }
}