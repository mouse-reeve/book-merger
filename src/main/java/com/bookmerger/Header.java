package com.bookmerger;

import org.apache.hadoop.io.Text;

public enum Header {
    AUTHOR_DETAILS,
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
    ORIGINALLANGUAGE,
    FROMWHERE,
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