/**
 * Copyright (C) 2013-2015 VCNC Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kr.co.vcnc.haeinsa;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

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

  private final HaeinsaTablePool haeinsaTablePool;
  private final HaeinsaTransactionManager transactionManager;
  private final Set<String> createdTableNames;

  private HaeinsaTestingCluster() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    HBaseTestingUtility utility = new HBaseTestingUtility(conf);
    cluster = utility.startMiniCluster();
    configuration = cluster.getConfiguration();

    haeinsaTablePool = TestingUtility.createHaeinsaTablePool(configuration);
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

  public Table getHbaseTable(String tableName) throws Exception {
    ensureTableCreated(tableName);
    return ConnectionFactory.createConnection(configuration).getTable(TableName.valueOf(tableName));
  }

  private synchronized void ensureTableCreated(String tableName) throws Exception {
    if (createdTableNames.contains(tableName)) {
      return;
    }
    Admin admin = ConnectionFactory.createConnection(configuration).getAdmin();
    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
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
    cluster.shutdown();
  }
}
