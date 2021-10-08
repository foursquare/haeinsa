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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.PoolMap.PoolType;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class HaeinsaTestingCluster {
    public static HaeinsaTestingCluster INSTANCE;

    public static HaeinsaTestingCluster getInstance() {
        synchronized (HaeinsaTestingCluster.class) {
            try {
                if (INSTANCE == null) {
                    INSTANCE = new HaeinsaTestingCluster();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return INSTANCE;
    }

    private final MiniHBaseCluster cluster;
    private final Configuration configuration;

    private final ExecutorService threadPool;
    private final HaeinsaTablePool haeinsaTablePool;
    private final HTablePool hbaseTablePool;
    private final HaeinsaTransactionManager transactionManager;
    private final Set<String> createdTableNames;

    private HaeinsaTestingCluster() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseTestingUtility utility = new HBaseTestingUtility(conf);
        utility.cleanupTestDir();
        cluster = utility.startMiniCluster();
        configuration = cluster.getConfiguration();

        threadPool = Executors.newCachedThreadPool();
        haeinsaTablePool = TestingUtility.createHaeinsaTablePool(configuration, threadPool);
        hbaseTablePool = new HTablePool(configuration, 128, PoolType.Reusable);
        transactionManager = new HaeinsaTransactionManager(haeinsaTablePool);
        createdTableNames = new HashSet<>();
    }

    public MiniHBaseCluster getCluster() {
        return cluster;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public HaeinsaTableIface getHaeinsaTable(String tableName) throws Exception {
        ensureTableCreated(tableName);
        return haeinsaTablePool.getTable(tableName);
    }

    public HTableInterface getHbaseTable(String tableName) throws Exception {
        ensureTableCreated(tableName);
        return hbaseTablePool.getTable(tableName);
    }

    private synchronized void ensureTableCreated(String tableName) throws Exception {
        if (createdTableNames.contains(tableName)) {
            return;
        }
        HBaseAdmin admin = new HBaseAdmin(configuration);
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        HColumnDescriptor lockColumnDesc = new HColumnDescriptor(HaeinsaConstants.LOCK_FAMILY);
        lockColumnDesc.setMaxVersions(1);
        lockColumnDesc.setInMemory(true);
        tableDesc.addFamily(lockColumnDesc);
        HColumnDescriptor dataColumnDesc = new HColumnDescriptor("data");
        tableDesc.addFamily(dataColumnDesc);
        HColumnDescriptor metaColumnDesc = new HColumnDescriptor("meta");
        tableDesc.addFamily(metaColumnDesc);
        HColumnDescriptor rawColumnDesc = new HColumnDescriptor("raw");
        tableDesc.addFamily(rawColumnDesc);
        admin.createTable(tableDesc);
        admin.close();

        createdTableNames.add(tableName);
    }

    public HaeinsaTransactionManager getTransactionManager() {
        return transactionManager;
    }

    public void release() throws IOException {
        threadPool.shutdown();
        cluster.shutdown();
    }
}
