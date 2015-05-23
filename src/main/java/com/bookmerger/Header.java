package com.bookmerger;

public enum Header {
    AUTHOR ("author", new String[] {"author_details"}),
    FORMAT("format", null),
    GENRE("genre", null),
    LIST_PRICE("list_price", null),
    PAGES ("pages", new String[] {"number of pages"}),
    PUBLISHER ("publisher", null),
    READ_START("date_added", null),
    READ_FINISH("date_read", null),
    SERIES ("series", new String[] {"series_details"}),
    TITLE ("title", null),
    ISBN ("isbn", null);

    private final String name;
    private final String[] synonyms;

    Header(String name, String[] synonyms) {
        this.name = name;
        this.synonyms = synonyms;
    }

    public String getName() {
        return this.name;
    }

    public static Header findHeader(String key) {
        key = key.toLowerCase();
        for (Header h : Header.values()) {
            if (key.equals(h.name)) {
                return h;
            }
            if (h.synonyms != null) {
                for (int i=0; i<h.synonyms.length; i++) {
                    if (h.synonyms[i].equals(key)) {
                        return h;
                    }
                }
            }
        }
        return null;
    }
}