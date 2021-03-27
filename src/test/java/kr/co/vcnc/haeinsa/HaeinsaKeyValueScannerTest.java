package kr.co.vcnc.haeinsa;

import com.google.common.collect.Lists;
import kr.co.vcnc.haeinsa.thrift.generated.TRowLock;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static kr.co.vcnc.haeinsa.HaeinsaKeyValueTest.shuffleCopy;

public class HaeinsaKeyValueScannerTest {

  @DataProvider(name = "comparator-provider")
  public Object[][] comparatorProvider() {
    return new Object[][]{{HaeinsaKeyValueScanner.COMPARATOR, false}, {HaeinsaKeyValueScanner.REVERSE_COMPARATOR, true}};
  }

  @Test(dataProvider = "comparator-provider")
  public void testComparatorTypeOrdering(Comparator<HaeinsaKeyValueScanner> comparator, boolean reverse) {
    List<HaeinsaKeyValueScanner> expected = toScanner(HaeinsaKeyValueTest.sameRowFamilyQualifierAllTypes());
    List<HaeinsaKeyValueScanner> actual = shuffleCopy(expected);
    actual.sort(comparator);
    Assert.assertEquals(actual, expected);
  }

  @Test(dataProvider = "comparator-provider")
  public void testComparatorRowOrdering(Comparator<HaeinsaKeyValueScanner> comparator, boolean reverse) {
    List<String> sorted = Arrays.asList("a", "b", "c", "d");
    if (reverse) {
      sorted = Lists.reverse(sorted);
    }
    List<HaeinsaKeyValueScanner> expected = sorted.stream()
        .map(row -> new SimpleScanner(new HaeinsaKeyValue(Bytes.toBytes(row), Bytes.toBytes("family"), Bytes.toBytes("qualifier"), Bytes.toBytes(row), KeyValue.Type.Put)))
        .collect(Collectors.toList());

    List<HaeinsaKeyValueScanner> actual = shuffleCopy(expected);
    actual.sort(comparator);
    Assert.assertEquals(actual, expected);
  }

  List<HaeinsaKeyValueScanner> toScanner(List<HaeinsaKeyValue> values) {
    return values.stream().map(SimpleScanner::new).collect(Collectors.toList());
  }

  private static class SimpleScanner implements HaeinsaKeyValueScanner {

    private final HaeinsaKeyValue value;

    private SimpleScanner(HaeinsaKeyValue value) {
      this.value = value;
    }

    @Override
    public HaeinsaKeyValue peek() {
      return value;
    }

    @Override
    public HaeinsaKeyValue next() throws IOException {
      throw new NotImplementedException();
    }

    @Override
    public TRowLock peekLock() {
      throw new NotImplementedException();
    }

    @Override
    public long getSequenceID() {
      return 0;
    }

    @Override
    public void close() {}

    @Override
    public String toString() {
      return peek().toString();
    }
  }

}
