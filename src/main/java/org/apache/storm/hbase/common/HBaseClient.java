/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.hbase.common;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HBaseClient {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseClient.class);

    private HTableInterface table;

    public HBaseClient(Map<String, Object> map , final Configuration configuration, final String tableName) {
        table = new HTablePool(configuration, Integer.MAX_VALUE).getTable(tableName);
    }

    public List<Row> constructMutationReq(byte[] rowKey, ColumnList cols, boolean durability) {
        List<Row> mutations = Lists.newArrayList();

        if (cols.hasColumns()) {
            Put put = new Put(rowKey);
            put.setWriteToWAL(durability);
            for (ColumnList.Column col : cols.getColumns()) {
                if (col.getTs() > 0) {
                    put.add(
                            col.getFamily(),
                            col.getQualifier(),
                            col.getTs(),
                            col.getValue()
                    );
                } else {
                    put.add(
                            col.getFamily(),
                            col.getQualifier(),
                            col.getValue()
                    );
                }
            }
            mutations.add(put);
        }

        if (cols.hasCounters()) {
            Increment inc = new Increment(rowKey);
            inc.setWriteToWAL(durability);
            for (ColumnList.Counter cnt : cols.getCounters()) {
                inc.addColumn(
                        cnt.getFamily(),
                        cnt.getQualifier(),
                        cnt.getIncrement()
                );
            }
            mutations.add(inc);
        }

        if (mutations.isEmpty()) {
            mutations.add(new Put(rowKey));
        }
        return mutations;
    }

    public void batchMutate(List<Row> mutations) throws Exception {
        Object[] result = new Object[mutations.size()];
        try {
            table.batch(mutations, result);
        } catch (InterruptedException e) {
            LOG.warn("Error performing a mutation to HBase.", e);
            throw e;
        } catch (IOException e) {
            LOG.warn("Error performing a mutation to HBase.", e);
            throw e;
        }
    }


    public Get constructGetRequests(byte[] rowKey, HBaseProjectionCriteria projectionCriteria) {
        Get get = new Get(rowKey);

        if (projectionCriteria != null) {
            for (byte[] columnFamily : projectionCriteria.getColumnFamilies()) {
                get.addFamily(columnFamily);
            }

            for (HBaseProjectionCriteria.ColumnMetaData columnMetaData : projectionCriteria.getColumns()) {
                get.addColumn(columnMetaData.getColumnFamily(), columnMetaData.getQualifier());
            }
        }

        return get;
    }

    public Result[] batchGet(List<Get> gets) throws Exception {
        try {
            return table.get(gets);
        } catch (Exception e) {
            LOG.warn("Could not perform HBASE lookup.", e);
            throw e;
        }
    }

    public Scan constructScanRequests(byte[] rowKey, HBaseProjectionCriteria projectionCriteria) {
        Scan scan = new Scan();

        if (projectionCriteria != null) {
            for (byte[] columnFamily : projectionCriteria.getColumnFamilies()) {
                scan.addFamily(columnFamily);
            }

            for (HBaseProjectionCriteria.ColumnMetaData columnMetaData : projectionCriteria.getColumns()) {
                scan.addColumn(columnMetaData.getColumnFamily(), columnMetaData.getQualifier());
            }
        }

        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(rowKey));
        scan.setFilter(filter);

        return scan;
    }

    public Result[] scan(Scan scan) throws Exception {
        try {
            List<Result> resultList = new ArrayList<Result>();

            for (Result r: table.getScanner(scan)) {
                resultList.add(r);
            }

            Result[] results = new Result[resultList.size()];
            results = resultList.toArray(results);

            return results;
        }
        catch (Exception e) {
            LOG.warn("Could not perform HBASE scan.", e);
            throw e;
        }
    }
}
