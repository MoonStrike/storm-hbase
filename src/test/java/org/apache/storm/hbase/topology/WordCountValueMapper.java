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
package org.apache.storm.hbase.topology;


import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

/**
 * Takes a Hbase result and returns a value list that has a value instance for each column and corresponding value.
 * So if the result from Hbase was
 * <pre>
 * WORD, COUNT
 * apple, 10
 * bannana, 20
 * </pre>
 *
 * this will return
 * <pre>
 *     [WORD, apple]
 *     [COUNT, 10]
 *     [WORD, banana]
 *     [COUNT, 20]
 * </pre>
 *
 */
public class WordCountValueMapper implements HBaseValueMapper {

    @Override
    public List<Values> toValues(Result result) throws Exception {
        List<Values> values = new ArrayList<Values>();
        NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = result.getNoVersionMap();

        for (byte[] family : map.keySet()) {
            NavigableMap<byte[], byte[]> cellMap = map.get(family);
            for (byte[] qualifier: cellMap.keySet()) {
                byte[] cellValue = cellMap.get(qualifier);
                Values value = new Values (Bytes.toString(qualifier), Bytes.toLong(cellValue));
                values.add(value);
            }
        }

        return values;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("columnName","columnValue"));
    }

}
