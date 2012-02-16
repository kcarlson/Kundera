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
package com.impetus.client.cassandra.pelops;

import java.lang.reflect.Field;
import java.util.Collection;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.cache.ElementCollectionCacheManager;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.EmbeddedColumn;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessorHelper;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.thrift.*;
import org.scale7.cassandra.pelops.Selector;

/**
 * Provides Pelops utility methods for data held in Column family based stores.
 *
 * @author amresh.singh
 */
public class PelopsDataHandler extends CassandraDataHandler
{

    /** The client. */
    private Client client;

    /**
     * Instantiates a new pelops data handler.
     *
     * @param client the client
     */
    public PelopsDataHandler(Client client)
    {
        super();
        this.client = client;
    }

    /** The log. */
    private static Log log = LogFactory.getLog(PelopsDataHandler.class);

    /**
     * From thrift row.
     *
     * @param selector the selector
     * @param clazz the clazz
     * @param m the m
     * @param rowKey the row key
     * @param relationNames the relation names
     * @param isWrapReq the is wrap req
     * @return the object
     * @throws Exception the exception
     */
    public Object fromThriftRow(Selector selector, Class<?> clazz, EntityMetadata m, String rowKey,
            List<String> relationNames, boolean isWrapReq) throws Exception
    {
        List<String> superColumnNames = m.getEmbeddedColumnFieldNames();
        Object e = null;

        if (!superColumnNames.isEmpty())
        {
            List<SuperColumn> thriftSuperColumns = selector.getSuperColumnsFromRow(m.getTableName(), rowKey, Selector
                    .newColumnsPredicateAll(true, 10000), ConsistencyLevel.ONE);
            e = fromSuperColumnThriftRow(clazz, m, new ThriftRow(rowKey, m.getTableName(), null, thriftSuperColumns),
                    relationNames, isWrapReq);

        }
        else
        {

            List<ByteBuffer> rowKeys = new ArrayList<ByteBuffer>(1);
            ByteBuffer rKeyAsByte = ByteUtils.stringToByteBuffer(rowKey);
            rowKeys.add(rKeyAsByte);

            Map<ByteBuffer, List<ColumnOrSuperColumn>> columnOrSuperColumnsFromRow = selector
                    .getColumnOrSuperColumnsFromRows(new ColumnParent(m.getTableName()), rowKeys, Selector
                            .newColumnsPredicateAll(true, 10000), ConsistencyLevel.ONE);

            List<ColumnOrSuperColumn> colList = columnOrSuperColumnsFromRow.get(rKeyAsByte);

            List<Column> thriftColumns = new ArrayList<Column>(colList.size());
            for (ColumnOrSuperColumn col : colList)
            {
                if (col.super_column == null)
                {
                    thriftColumns.add(col.getColumn());
                }
                else
                {
                    thriftColumns.addAll(col.getSuper_column().getColumns());
                }

            }

            e = fromColumnThriftRow(clazz, m, new ThriftRow(rowKey, m.getTableName(), thriftColumns, null),
                    relationNames, isWrapReq);
        }

        return e;
    }

    /**
     * From thrift row.
     *
     * @param selector the selector
     * @param clazz the clazz
     * @param m the m
     * @param relationNames the relation names
     * @param isWrapReq the is wrap req
     * @param rowIds the row ids
     * @return the list
     * @throws Exception the exception
     */
    public List<Object> fromThriftRow(Selector selector, Class<?> clazz, EntityMetadata m, List<String> relationNames,
            boolean isWrapReq, String... rowIds) throws Exception
    {
        List<Object> entities = new ArrayList<Object>(rowIds.length);

        for (String rowKey : rowIds)
        {
            Object e = fromThriftRow(selector, clazz, m, rowKey, relationNames, isWrapReq);
            entities.add(e);
        }

        return entities;
    }

    /**
     * Helper method to convert @Entity to ThriftRow.
     *
     * @param client the client
     * @param e the e
     * @param id the id
     * @param m the m
     * @param columnFamily the colmun family
     * @return the base data accessor. thrift row
     * @throws Exception the exception
     */
    public ThriftRow toThriftRow(PelopsClient client, Object e, String id, EntityMetadata m, String columnFamily)
            throws Exception
    {
        // timestamp to use in thrift column objects
        // long timestamp = System.currentTimeMillis();

        ThriftRow tr = new ThriftRow();

        tr.setColumnFamilyName(columnFamily); // column-family name
        tr.setId(id); // Id

        // Add super columns to thrift row
        addSuperColumnsToThriftRow(timestamp, client, tr, m, e, id);

        // Add columns to thrift row, only if there is no super column
        if (m.getEmbeddedColumnsAsList().isEmpty())
        {
            addColumnsToThriftRow(timestamp, tr, m, e);
        }

        // Add relations entities as Foreign keys to a new super column created
        // internally
        // addRelationshipsToThriftRow(timestamp, tr, e, m);

        return tr;
    }

    /**
     * Adds the super columns to thrift row.
     *
     * @param timestamp the timestamp
     * @param client the client
     * @param tr the tr
     * @param m the m
     * @param e the e
     * @param id the id
     * @throws Exception the exception
     */
    private void addSuperColumnsToThriftRow(long timestamp, PelopsClient client, ThriftRow tr, EntityMetadata m,
            Object e, String id) throws Exception
    {
        // Iterate through Super columns
        for (EmbeddedColumn superColumn : m.getEmbeddedColumnsAsList())
        {
            Field superColumnField = superColumn.getField();
            Object superColumnObject = PropertyAccessorHelper.getObject(e, superColumnField);

            // If Embedded object is a Collection, there will be variable number
            // of super columns one for each object in collection.
            // Key for each super column will be of the format "<Embedded object
            // field name>#<Unique sequence number>
            // On the other hand, if embedded object is not a Collection, it
            // would simply be embedded as ONE super column.
            String superColumnName = null;

            if (superColumnObject == null)
            {
                continue;
            }

            if (superColumnObject instanceof Collection)
            {
                ElementCollectionCacheManager ecCacheHandler = ElementCollectionCacheManager.getInstance();

                // Check whether it's first time insert or updation
                if (ecCacheHandler.isCacheEmpty())
                { // First time insert
                    int count = 0;

                    for (Object obj : (Collection) superColumnObject)
                    {
                        superColumnName = superColumn.getName() + Constants.EMBEDDED_COLUMN_NAME_DELIMITER + count;

                        SuperColumn thriftSuperColumn = buildThriftSuperColumn(superColumnName, timestamp, superColumn,
                                obj);

                        tr.addSuperColumn(thriftSuperColumn);
                        count++;
                    }
                }
                else
                {
                    // Updation, Check whether this object is already in cache,
                    // which means we already have a super column
                    // Otherwise we need to generate a fresh embedded column
                    // name
                    int lastEmbeddedObjectCount = ecCacheHandler.getLastElementCollectionObjectCount(id);

                    for (Object obj : (Collection) superColumnObject)
                    {
                        superColumnName = ecCacheHandler.getElementCollectionObjectName(id, obj);

                        if (superColumnName == null)
                        { // Fresh row
                            superColumnName = superColumn.getName() + Constants.EMBEDDED_COLUMN_NAME_DELIMITER
                                    + (++lastEmbeddedObjectCount);
                        }

                        SuperColumn thriftSuperColumn = buildThriftSuperColumn(superColumnName, timestamp, superColumn,
                                obj);

                        tr.addSuperColumn(thriftSuperColumn);
                    }
                }
            }
            else
            {
                superColumnName = superColumn.getName();

                SuperColumn thriftSuperColumn = buildThriftSuperColumn(superColumnName, timestamp, superColumn,
                        superColumnObject);

                tr.addSuperColumn(thriftSuperColumn);
            }
        }
    }

}
