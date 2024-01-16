package com.s82033788.CPEN431.A4;

import java.util.Arrays;

public class KeyWrapper {
    byte[] key;

    public KeyWrapper(byte[] key) {
        this.key = key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyWrapper that = (KeyWrapper) o;
        return Arrays.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(key);
    }
}
