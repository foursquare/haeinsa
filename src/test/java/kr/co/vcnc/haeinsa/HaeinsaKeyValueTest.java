package kr.co.vcnc.haeinsa;

import org.apache.hadoop.hbase.KeyValue;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.apache.hadoop.hbase.KeyValue.Type.*;

public class HaeinsaKeyValueTest {
  private static final List<KeyValue.Type> TRAVERSAL_ORDER = Arrays.asList(Maximum, DeleteFamily, DeleteColumn, DeleteFamilyVersion, Delete, Put, Minimum);

  @Test
  public void testComparatorTypeOrdering() {
    comparatorTypeOrdering(HaeinsaKeyValue.COMPARATOR);
  }

  @Test
  public void testReverseTypeOrdering() {
    comparatorTypeOrdering(HaeinsaKeyValue.REVERSE_COMPARATOR);
  }

  public void comparatorTypeOrdering(Comparator<HaeinsaKeyValue> comparator) {
    List<HaeinsaKeyValue> expected = sameRowFamilyQualifierAllTypes();
    List<HaeinsaKeyValue> actual = new ArrayList<>(expected);

    Collections.sort(actual, comparator);

    Assert.assertEquals(actual, expected);
  }

  List<HaeinsaKeyValue> sameRowFamilyQualifierAllTypes() {
    ArrayList<HaeinsaKeyValue> all = new ArrayList<>();
    for (KeyValue.Type type : TRAVERSAL_ORDER) {
      all.add(new HaeinsaKeyValue(bytes("row"), bytes("family"), bytes("qualifier"), bytes(type.toString()), type));
    }
    return all;
  }

  private byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }
}
