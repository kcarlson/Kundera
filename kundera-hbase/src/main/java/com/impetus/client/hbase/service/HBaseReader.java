/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.client.hbase.service;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.client.hbase.HBaseData;
import com.impetus.client.hbase.Reader;

/**
 * HBase reader.
 * 
 * @author impetus
 */
public class HBaseReader implements Reader
{

    /* (non-Javadoc)
     * @see com.impetus.client.hbase.Reader#LoadData(org.apache.hadoop.hbase.client.HTable, java.lang.String, java.lang.String)
     */
    @SuppressWarnings("unused")
    @Override
    public HBaseData LoadData(HTable hTable, String columnFamily, String rowKey) throws IOException
    {
        HBaseData data = new HBaseData(columnFamily, rowKey);

        Get g = new Get(Bytes.toBytes(rowKey));

        Result r = hTable.get(g);

        // TODO initially targeting to get all values on the basis for give row
        // key and column family.
        // RowResult rwResult = r.getRowResult();
        List<KeyValue> values = r.list();
        data.setColumns(values);
        return data;
    }

    /* (non-Javadoc)
     * @see com.impetus.client.hbase.Reader#LoadData(org.apache.hadoop.hbase.client.HTable, java.lang.String)
     */
    @Override
    public HBaseData LoadData(HTable hTable, String rowKey) throws IOException
    {
        HBaseData data = new HBaseData(rowKey);

        Get g = new Get(Bytes.toBytes(rowKey));

        Result r = hTable.get(g);

        List<KeyValue> values = r.list();
        data.setColumns(values);
        return data;
    }

    // TODO: for first version this solution is for 1 column family per table
    // Later need to add support for more than 1 column family.
    /* (non-Javadoc)
     * @see com.impetus.client.hbase.Reader#loadAll(org.apache.hadoop.hbase.client.HTable, java.lang.String[])
     */
    @Override
    public HBaseData loadAll(HTable hTable, String... qualifiers) throws IOException
    {
        String rowKey;
        String columnFamily;
        HBaseData data = null;
        Scan s = new Scan();
        ResultScanner scanner = hTable.getScanner(s);
        for (Result rr : scanner)
        {
            for (KeyValue rs : rr.list())
            {
                rowKey = Bytes.toString(rs.getRow());
                columnFamily = Bytes.toString(rs.getFamily());
                data = new HBaseData(columnFamily, rowKey);
                data.setColumns(rr.list());
                break;
            }
        }
        return data;
    }
}
