package org.urbcomp.startdb.stkq.filter;

import com.github.nivdayan.FilterLibrary.filters.ChainedInfiniFilter;
import org.apache.lucene.util.RamUsageEstimator;
import org.urbcomp.startdb.stkq.keyGenerator.HilbertSpatialKeyGeneratorNew;
import org.urbcomp.startdb.stkq.keyGenerator.ISpatialKeyGeneratorNew;
import org.urbcomp.startdb.stkq.keyGenerator.KeywordKeyGeneratorNew;
import org.urbcomp.startdb.stkq.keyGenerator.TimeKeyGeneratorNew;

import java.util.ArrayList;
import java.util.List;

public class BasicRosetta {
    protected int n;
    protected List<ChainedInfiniFilter> filters;
    ISpatialKeyGeneratorNew sKeyGenerator = new HilbertSpatialKeyGeneratorNew();
    TimeKeyGeneratorNew tKeyGenerator = new TimeKeyGeneratorNew();
    KeywordKeyGeneratorNew kKeyGenerator = new KeywordKeyGeneratorNew();

    public BasicRosetta(int n) {
        this.n = n;
        filters = new ArrayList<>(n);
        for (int i = 0; i < n; ++i) {
            ChainedInfiniFilter filter = new ChainedInfiniFilter(3, 15);
            filter.set_expand_autonomously(true);
            filters.add(filter);
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(n);
        for (int i = 0; i < n; ++i) {
            builder.append(" ").append(RamUsageEstimator.humanSizeOf(filters.get(i)));
        }
        return builder.toString();
    }
}
