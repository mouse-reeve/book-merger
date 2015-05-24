package com.bookmerger;

import org.apache.hadoop.io.Text;

public enum Header {
    AUTHORS,
    FORMAT,
    GENRE,
    LIST_PRICE,
    PAGES,
    PUBLISHER,
    DATE_ADDED,
    DATE_PUBLISHED,
    SERIES,
    TITLE,
    LANGUAGE,
    ORIGINAL_LANGUAGE,
    FROM_WHERE,
    PLACES,
    EVENTS,
    CHARACTERS,
    ISBN;

    public Text asText() {
        return new Text(this.toString());
    }

    @Override public String toString() {
        return super.toString().toLowerCase();
    }
}