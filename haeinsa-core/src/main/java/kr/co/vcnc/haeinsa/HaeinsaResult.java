/**
 * Copyright (C) 2013-2015 VCNC Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kr.co.vcnc.haeinsa;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Result;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * Modified POJO container of {@link Result} class in HBase. Link {@link Result}
 * , can contain multiple {@link HaeinsaKeyValue}. All HaeinsaKeyValue in
 * HaeinsaResult are from same row.
 */
public class HaeinsaResult {
    private final List<HaeinsaKeyValue> sortedKVs;
    private byte[] row;

    /**
     * Construct HaeinsaResult from Result
     *
     * @param result HBase's result
     */
    public HaeinsaResult(Result result) {
        if (result.isEmpty()) {
            List<HaeinsaKeyValue> emptyList = Collections.emptyList();
            this.sortedKVs = emptyList;
        } else {
            List<HaeinsaKeyValue> transformed = Lists.transform(
                    result.listCells(),
                    new Function<Cell, HaeinsaKeyValue>() {
                        @Override
                        public HaeinsaKeyValue apply(Cell kv) {
                            return new HaeinsaKeyValue(KeyValueUtil.ensureKeyValue(kv));
                        }
                    });
            this.sortedKVs = transformed;
        }
    }

    /**
     * Construct HaeinsaResultImpl from sorted list of HaeinsaKeyValue
     *
     * @param sortedKVs - Assumed that {@link HaeinsaKeyValue}s in sortedKVs
     * have same row with first element and sorted in ascending order.
     */
    public HaeinsaResult(List<HaeinsaKeyValue> sortedKVs) {
        this.sortedKVs = sortedKVs;
        if (sortedKVs.size() > 0) {
            row = sortedKVs.get(0).getRow();
        }
    }

    public byte[] getRow() {
        return row;
    }

    public List<HaeinsaKeyValue> list() {
        return sortedKVs;
    }

    public byte[] getValue(byte[] family, byte[] qualifier) {
        // pos === ( -(insertion point) - 1)
        int pos = Collections.binarySearch(sortedKVs,
                new HaeinsaKeyValue(row, family, qualifier, null, KeyValue.Type.Maximum), HaeinsaKeyValue.COMPARATOR);
        // never will exact match
        if (pos < 0) {
            pos = (pos + 1) * -1;
            // pos is now insertion point
        }
        if (pos == sortedKVs.size()) {
            return null;
        }
        HaeinsaKeyValue kv = sortedKVs.get(pos);
        if (kv.matchingColumn(family, qualifier)) {
            return kv.getValue();
        }
        return null;
    }

    public boolean containsColumn(byte[] family, byte[] qualifier) {
        return getValue(family, qualifier) != null;
    }

    public boolean isEmpty() {
        return sortedKVs.size() == 0;
    }
}
