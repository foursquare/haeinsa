package kr.co.vcnc.haeinsa;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.hadoop.hbase.KeyValue.Type.*;

public class HaeinsaKeyValueTest {
  public static final List<KeyValue.Type> TRAVERSAL_ORDER = Arrays.asList(Maximum, DeleteFamily, DeleteColumn, DeleteFamilyVersion, Delete, Put, Minimum);

  @DataProvider(name = "comparator-provider")
  public Object[][] comparatorProvider() {
    return new Object[][]{{HaeinsaKeyValue.COMPARATOR}, {HaeinsaKeyValue.REVERSE_COMPARATOR}};
  }

  @Test(dataProvider = "comparator-provider")
  public void testComparatorTypeOrdering(Comparator<HaeinsaKeyValue> comparator) {
    List<HaeinsaKeyValue> expected = sameRowFamilyQualifierAllTypes();
    List<HaeinsaKeyValue> actual = shuffleCopy(expected);

    actual.sort(comparator);

    Assert.assertEquals(actual, expected);
  }

  public static List<HaeinsaKeyValue> sameRowFamilyQualifierAllTypes() {
    return TRAVERSAL_ORDER.stream()
        .map(type -> new HaeinsaKeyValue(Bytes.toBytes("row"), Bytes.toBytes("family"), Bytes.toBytes("qualifier"), Bytes.toBytes(type.toString()), type))
        .collect(Collectors.toList());
  }

  public static <T> List<T> shuffleCopy(List<T> items) {
    ArrayList<T> newItems = new ArrayList<>(items);
    Collections.shuffle(newItems, new Random(1));
    return newItems;
  }
}
