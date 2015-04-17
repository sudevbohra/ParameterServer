package org.petuum.ps.common.util;

public class PtrBox<T> {
    public T value;

    public PtrBox() {
        value = null;
    }

    public PtrBox(T i) {
        value = i;
    }
}
