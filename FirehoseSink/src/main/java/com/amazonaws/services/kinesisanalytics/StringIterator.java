package com.amazonaws.services.kinesisanalytics;

import java.io.Serializable;
import java.util.Iterator;

public class StringIterator implements Iterator<String>, Serializable {

    private static final long serialVersionUID = 1L;

    private int index;

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public String next() {
        return "foo-bar-" + ++index;
    }

}
