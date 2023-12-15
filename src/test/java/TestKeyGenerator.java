import org.junit.Test;
import org.urbcomp.startdb.stkq.preProcessing.DataProcessor;
import org.urbcomp.startdb.stkq.keyGenerator.ISTKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.STKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.TSKeyGenerator;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.DateUtil;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class TestKeyGenerator {

    private static final List<STObject> SAMPLE_DATA = DataProcessor.getSampleData();

    @Test
    public void testCorrectness() {
        testSTKeyGenerator(new STKeyGenerator());
        testSTKeyGenerator(new TSKeyGenerator());
    }

    private void testSTKeyGenerator(ISTKeyGenerator keyGenerator) {
        for (STObject object : SAMPLE_DATA) {
            long stKey = keyGenerator.toNumber(object);
            List<Range<Long>> stRanges = keyGenerator.toNumberRanges(getQueryByObject(object));
            boolean find = false;
            for (Range<Long> stRange : stRanges) {
                if (stKey >= stRange.getLow() && stKey <= stRange.getHigh()) {
                    find = true;
                    break;
                }
            }
            assertTrue(find);
        }
    }

    private Query getQueryByObject(STObject object) {
        return new Query(
                object.getLat() - 0.01,
                object.getLat() + 0.01,
                object.getLon() - 0.01,
                object.getLon() + 0.01,
                DateUtil.getDateAfterMinutes(object.getTime(), -30),
                DateUtil.getDateAfterMinutes(object.getTime(), 20),
                new ArrayList<>()
        );
    }
}