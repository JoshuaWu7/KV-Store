package com.s82033788.CPEN431.A4;

import java.util.Arrays;
import java.util.Objects;

public class ValueWrapper {
    byte[] value;
    int version;

    public ValueWrapper(byte[] value, int version) {
        this.value = value;
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValueWrapper that = (ValueWrapper) o;
        return version == that.version && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(version);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }
}
