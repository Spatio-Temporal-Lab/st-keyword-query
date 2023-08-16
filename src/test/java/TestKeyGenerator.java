import junit.framework.TestCase;
import org.urbcomp.startdb.stkq.keyGenerator.ISTKeyGeneratorNew;
import org.urbcomp.startdb.stkq.keyGenerator.STKeyGenerator;
import org.urbcomp.startdb.stkq.keyGenerator.TSKeyGenerator;
import org.urbcomp.startdb.stkq.model.Query;
import org.urbcomp.startdb.stkq.model.Range;
import org.urbcomp.startdb.stkq.model.STObject;
import org.urbcomp.startdb.stkq.util.DateUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class TestKeyGenerator extends TestCase {

    private static final List<STObject> SAMPLE_DATA = getSampleData();
    private static final String TWEET_SAMPLE_FILE = "src/main/resources/tweetSample.csv";

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

    public void testCorrectness() {
        testSTKeyGenerator(new STKeyGenerator());
        testSTKeyGenerator(new TSKeyGenerator());
    }

    private void testSTKeyGenerator(ISTKeyGeneratorNew keyGenerator) {
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

    private static List<STObject> getSampleData() {
        List<STObject> objects = new ArrayList<>();
        try (BufferedReader in = new BufferedReader(new FileReader(TWEET_SAMPLE_FILE))) {
            String line;
            while ((line = in.readLine()) != null) {
                objects.add(new STObject(line));
            }
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
        return objects;
    }
}