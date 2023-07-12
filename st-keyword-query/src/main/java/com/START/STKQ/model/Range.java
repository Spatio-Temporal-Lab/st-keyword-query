package com.START.STKQ.model;

import java.util.Objects;

public class Range<T> {
    private T low;
    private T high;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Range<?> range = (Range<?>) o;
        return Objects.equals(low, range.low) && Objects.equals(high, range.high);
    }

    @Override
    public int hashCode() {
        return Objects.hash(low, high);
    }

    public Range(T low, T high) {
        this.low = low;
        this.high = high;
    }

    public T getHigh() {
        return high;
    }

    public T getLow() {
        return low;
    }

    public void setLow(T low) {
        this.low = low;
    }

    public void setHigh(T high) {
        this.high = high;
    }

    @Override
    public String toString() {
        return low.toString() + " " + high.toString();
    }
}
