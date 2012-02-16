/**
 * *****************************************************************************
 * * Copyright 2011 Impetus Infotech. * * Licensed under the Apache License,
 * Version 2.0 (the "License"); * you may not use this file except in compliance
 * with the License. * You may obtain a copy of the License at * *
 * http://www.apache.org/licenses/LICENSE-2.0 * * Unless required by applicable
 * law or agreed to in writing, software * distributed under the License is
 * distributed on an "AS IS" BASIS, * WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. * See the License for the specific language
 * governing permissions and * limitations under the License.
 *****************************************************************************
 */
package com.impetus.client.cassandra.pelops;

import com.impetus.kundera.cache.ElementCollectionCacheManager;
import com.impetus.kundera.client.DataHandler;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EmbeddedColumn;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.*;
import javax.persistence.PersistenceException;
import org.apache.cassandra.thrift.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Selector;
import sun.awt.windows.ThemeReader;

/**
 *
 * @author amresh.singh/kcarlson
 */
public class CassandraDataHandler extends DataHandler
{

    /**
     * The log.
     */
    protected static Log log = LogFactory.getLog(CassandraDataHandler.class);

    /** The timestamp. */
    protected long timestamp = System.currentTimeMillis();

    /**
     * From thrift row.
     *
     * @param <E>
     *            the element type
     * @param clazz
     *            the clazz
     * @param m
     *            the m
     * @param tr
     *            the cr
     * @return the e
     * @throws Exception
     *             the exception
     */

    // TODO: this is a duplicate code snippet and we need to refactor this.
    public <E> E fromThriftRow(Class<E> clazz, EntityMetadata m, DataRow<SuperColumn> tr) throws Exception
    {
        // Instantiate a new instance
        E e = null;

        // Set row-key. Note:
        // PropertyAccessorHelper.setId(e, m, tr.getId());

        // Get a name->field map for super-columns
        Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();
        Map<String, Field> superColumnNameToFieldMap = new HashMap<String, Field>();

        MetadataUtils.populateColumnAndSuperColumnMaps(m, columnNameToFieldMap, superColumnNameToFieldMap);

        Collection embeddedCollection = null;
        Field embeddedCollectionField = null;

        for (SuperColumn sc : tr.getColumns())
        {
            if (e == null)
            {
                // Instantiate a new instance
                e = clazz.newInstance();

                // Set row-key. Note:
                PropertyAccessorHelper.setId(e, m, tr.getId());
            }

            String scName = PropertyAccessorFactory.STRING.fromBytes(sc.getName());
            String scNamePrefix = null;

            if (scName.indexOf(com.impetus.kundera.Constants.EMBEDDED_COLUMN_NAME_DELIMITER) != -1)
            {
                scNamePrefix = MetadataUtils.getEmbeddedCollectionPrefix(scName);
                embeddedCollectionField = superColumnNameToFieldMap.get(scNamePrefix);
                embeddedCollection = MetadataUtils.getEmbeddedCollectionInstance(embeddedCollectionField);

                Object embeddedObject = populateEmbeddedObject(sc, m);

                embeddedCollection.add(embeddedObject);
                PropertyAccessorHelper.set(e, embeddedCollectionField, embeddedCollection);
            }
            else
            {
                boolean intoRelations = false;

                if (scName.equals(com.impetus.kundera.Constants.FOREIGN_KEY_EMBEDDED_COLUMN_NAME))
                {
                    intoRelations = true;
                }

                for (Column column : sc.getColumns())
                {
                    String name = PropertyAccessorFactory.STRING.fromBytes(column.getName());
                    byte[] value = column.getValue();

                    if (value == null)
                    {
                        continue;
                    }

                    if (intoRelations)
                    {
                        Relation relation = m.getRelation(name);
                        String foreignKeys = PropertyAccessorFactory.STRING.fromBytes(value);
                        Set<String> keys = MetadataUtils.deserializeKeys(foreignKeys);
                    }
                    else
                    {
                        // set value of the field in the bean
                        Field field = columnNameToFieldMap.get(name);
                        Object embeddedObject = PropertyAccessorHelper.getObject(e, scName);

                        PropertyAccessorHelper.set(embeddedObject, field, value);
                    }
                }
            }
        }

        return e;
    }

    /**
     * Fetches data held in Thrift row columns and populates to Entity objects.
     *
     * @param clazz the clazz
     * @param m the m
     * @param thriftRow the cr
     * @param relationNames the relation names
     * @param isWrapperReq the is wrapper req
     * @return the e
     * @throws Exception the exception
     */
    public Object fromColumnThriftRow(Class<?> clazz, EntityMetadata m, ThriftRow thriftRow,
            List<String> relationNames, boolean isWrapperReq) throws Exception
    {
        // Instantiate a new instance
        Object entity = null;
        Map<String, Object> relations = new HashMap<String, Object>();

        // Set row-key.
        // PropertyAccessorHelper.setId(entity, m, thriftRow.getId());
        // PropertyAccessorHelper.set(entity, m.getIdColumn().getField(),
        // thriftRow.getId());

        // Iterate through each column
        for (Column c : thriftRow.getColumns())
        {
            if (entity == null)
            {
                entity = clazz.newInstance();
                // Set row-key
                PropertyAccessorHelper.setId(entity, m, thriftRow.getId());
            }

            String thriftColumnName = PropertyAccessorFactory.STRING.fromBytes(c.getName());

            // KEY is row key in cql result.
            if ("KEY".equals(thriftColumnName))
            {
                continue;
            }

            byte[] thriftColumnValue = c.getValue();

            if (null == thriftColumnValue)
            {
                continue;
            }

            // Check if this is a property, or a column representing foreign
            // keys
            com.impetus.kundera.metadata.model.Column column = m.getColumn(thriftColumnName);
            if (column != null)
            {
                try
                {
                    PropertyAccessorHelper.set(entity, column.getField(), thriftColumnValue);
                }
                catch (PropertyAccessException pae)
                {
                    log.warn(pae.getMessage());
                }
            }
            else
            {
                if (relationNames != null && !relationNames.isEmpty() && relationNames.contains(thriftColumnName))
                {
                    // relations = new HashMap<String, Object>();
                    String value = PropertyAccessorFactory.STRING.fromBytes(thriftColumnValue);
                    relations.put(thriftColumnName, value);
                    // prepare EnhanceEntity and return it
                }
            }
        }
        return isWrapperReq ? new EnhanceEntity(entity, thriftRow.getId(), relations) : entity;
    }

    /**
     * Fetches data held in Thrift row super columns and populates to Entity
     * objects.
     *
     * @param clazz the clazz
     * @param m the m
     * @param tr the tr
     * @param relationNames the relation names
     * @param isWrapReq the is wrap req
     * @return the object
     * @throws Exception the exception
     */
    public Object fromSuperColumnThriftRow(Class clazz, EntityMetadata m, ThriftRow tr, List<String> relationNames,
            boolean isWrapReq) throws Exception
    {
        // Instantiate a new instance
        Object entity = null;

        // Map to hold property-name=>foreign-entity relations
        Map<String, Set<String>> foreignKeysMap = new HashMap<String, Set<String>>();

        // Get a name->field map for super-columns
        Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();
        Map<String, Field> superColumnNameToFieldMap = new HashMap<String, Field>();

        MetadataUtils.populateColumnAndSuperColumnMaps(m, columnNameToFieldMap, superColumnNameToFieldMap);

        // Add all super columns to entity
        Collection embeddedCollection = null;
        Field embeddedCollectionField = null;
        Map<String, Object> relations = new HashMap<String, Object>();

        for (SuperColumn sc : tr.getSuperColumns())
        {
            if (entity == null)
            {
                entity = clazz.newInstance();
                // Set row-key
                PropertyAccessorHelper.setId(entity, m, tr.getId());
            }
            String scName = PropertyAccessorFactory.STRING.fromBytes(sc.getName());
            String scNamePrefix = null;

            // If this super column is variable in number (name#sequence format)
            if (scName.indexOf(com.impetus.kundera.Constants.EMBEDDED_COLUMN_NAME_DELIMITER) != -1)
            {
                scNamePrefix = MetadataUtils.getEmbeddedCollectionPrefix(scName);
                embeddedCollectionField = superColumnNameToFieldMap.get(scNamePrefix);

                if (embeddedCollection == null)
                {
                    embeddedCollection = MetadataUtils.getEmbeddedCollectionInstance(embeddedCollectionField);
                }

                Object embeddedObject = MetadataUtils.getEmbeddedGenericObjectInstance(embeddedCollectionField);

                for (Column column : sc.getColumns())
                {
                    String name = PropertyAccessorFactory.STRING.fromBytes(column.getName());
                    byte[] value = column.getValue();

                    if (value == null)
                    {
                        continue;
                    }

                    Field columnField = columnNameToFieldMap.get(name);
                    if (columnField != null)
                    {
                        PropertyAccessorHelper.set(embeddedObject, columnField, value);
                    }
                    else if (relationNames != null && !relationNames.isEmpty() && relationNames.contains(name))
                    {
                        String valueAsStr = PropertyAccessorFactory.STRING.fromBytes(value);
                        relations.put(name, valueAsStr);
                    }
                }

                embeddedCollection.add(embeddedObject);

                // Add this embedded object to cache
                ElementCollectionCacheManager.getInstance().addElementCollectionCacheMapping(tr.getId(),
                        embeddedObject, scName);
            }
            else
            {
                // For embedded super columns, create embedded entities and
                // add them to parent entity
                Field superColumnField = superColumnNameToFieldMap.get(scName);
                Object superColumnObj = null;
                if (superColumnField != null
                        || (relationNames != null && !relationNames.isEmpty() && relationNames.contains(scName)))
                {

                    Class superColumnClass = superColumnField != null ? superColumnField.getType() : null;
                    superColumnObj = superColumnClass != null ? superColumnClass.newInstance() : null;
                    for (Column column : sc.getColumns())
                    {
                        String name = PropertyAccessorFactory.STRING.fromBytes(column.getName());
                        byte[] value = column.getValue();
                        Field columnField = columnNameToFieldMap.get(name);
                        if (columnField != null)
                        {
                            try
                            {
                                PropertyAccessorHelper.set(superColumnObj, columnField, value);
                            }
                            catch (PropertyAccessException e)
                            {
                                // This is an entity column to be retrieved in a
                                // super column family. It's stored as a super
                                // column that would
                                // have just one column with the same name
                                log
                                        .debug(e.getMessage()
                                                + ". Possible case of entity column in a super column family. Will be treated as a super column.");
                                superColumnObj = Bytes.toUTF8(value);
                            }

                        }
                        else
                        {
                            String valueAsStr = PropertyAccessorFactory.STRING.fromBytes(value);
                            relations.put(name, valueAsStr);
                        }

                    }

                }

                if (superColumnField != null)
                {
                    PropertyAccessorHelper.set(entity, superColumnField, superColumnObj);
                }

            }
        }

        if ((embeddedCollection != null) && !embeddedCollection.isEmpty())
        {
            PropertyAccessorHelper.set(entity, embeddedCollectionField, embeddedCollection);
        }

        // EnhancedEntity e = EntityResolver.getEnhancedEntity(entity,
        // tr.getId(), foreignKeysMap);
        return isWrapReq ? new EnhanceEntity(entity, tr.getId(), relations) : entity;
    }

    /**
     * Populate embedded object.
     *
     * @param sc the sc
     * @param m the m
     * @return the object
     * @throws Exception the exception
     */
    public Object populateEmbeddedObject(SuperColumn sc, EntityMetadata m) throws Exception
    {
        Field embeddedCollectionField = null;
        Object embeddedObject = null;
        String scName = PropertyAccessorFactory.STRING.fromBytes(sc.getName());
        String scNamePrefix = null;

        // Get a name->field map for super-columns
        Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();
        Map<String, Field> superColumnNameToFieldMap = new HashMap<String, Field>();

        MetadataUtils.populateColumnAndSuperColumnMaps(m, columnNameToFieldMap, superColumnNameToFieldMap);

        // If this super column is variable in number (name#sequence format)
        if (scName.indexOf(com.impetus.kundera.Constants.EMBEDDED_COLUMN_NAME_DELIMITER) != -1)
        {
            StringTokenizer st = new StringTokenizer(scName,
                    com.impetus.kundera.Constants.EMBEDDED_COLUMN_NAME_DELIMITER);

            if (st.hasMoreTokens())
            {
                scNamePrefix = st.nextToken();
            }

            embeddedCollectionField = superColumnNameToFieldMap.get(scNamePrefix);

            Class<?> embeddedClass = PropertyAccessorHelper.getGenericClass(embeddedCollectionField);

            // must have a default no-argument constructor
            try
            {
                embeddedClass.getConstructor();
            }
            catch (NoSuchMethodException nsme)
            {
                throw new PersistenceException(embeddedClass.getName()
                        + " is @Embeddable and must have a default no-argument constructor.");
            }

            embeddedObject = embeddedClass.newInstance();

            for (Column column : sc.getColumns())
            {
                String name = PropertyAccessorFactory.STRING.fromBytes(column.getName());
                byte[] value = column.getValue();

                if (value == null)
                {
                    continue;
                }

                Field columnField = columnNameToFieldMap.get(name);

                PropertyAccessorHelper.set(embeddedObject, columnField, value);
            }
        }
        else
        {
            Field superColumnField = superColumnNameToFieldMap.get(scName);
            Class superColumnClass = superColumnField.getType();

            embeddedObject = superColumnClass.newInstance();

            for (Column column : sc.getColumns())
            {
                String name = PropertyAccessorFactory.STRING.fromBytes(column.getName());
                byte[] value = column.getValue();

                if (value == null)
                {
                    continue;
                }

                // set value of the field in the bean
                Field columnField = columnNameToFieldMap.get(name);

                PropertyAccessorHelper.set(embeddedObject, columnField, value);
            }
        }

        return embeddedObject;
    }

    /**
     * Adds the columns to thrift row.
     *
     * @param timestamp the timestamp
     * @param tr the tr
     * @param m the m
     * @param e the e
     * @throws Exception the exception
     */
    protected void addColumnsToThriftRow(long timestamp, ThriftRow tr, EntityMetadata m, Object e) throws Exception
    {
        List<Column> columns = new ArrayList<Column>();

        // Iterate through each column-meta and populate that with field values
        for (com.impetus.kundera.metadata.model.Column column : m.getColumnsAsList())
        {
            Field field = column.getField();
            if (field.getType().isAssignableFrom(Set.class) || field.getType().isAssignableFrom(Collection.class))
            {
            }
            else
            {
                String name = column.getName();
                try
                {
                    byte[] value = PropertyAccessorHelper.get(e, field);
                    Column col = new Column();
                    col.setName(PropertyAccessorFactory.STRING.toBytes(name));
                    col.setValue(value);
                    col.setTimestamp(timestamp);
                    columns.add(col);
                }
                catch (PropertyAccessException exp)
                {
                    log.warn(exp.getMessage());
                }
            }
        }

        tr.setColumns(columns);
    }

    /**
     * Builds the thrift super column.
     *
     * @param superColumnName the super column name
     * @param timestamp the timestamp
     * @param superColumn the super column
     * @param superColumnObject the super column object
     * @return the super column
     * @throws PropertyAccessException the property access exception
     */
    protected SuperColumn buildThriftSuperColumn(String superColumnName, long timestamp, EmbeddedColumn superColumn,
            Object superColumnObject) throws PropertyAccessException
    {
        List<Column> thriftColumns = new ArrayList<Column>();

        for (com.impetus.kundera.metadata.model.Column column : superColumn.getColumns())
        {
            Field field = column.getField();
            String name = column.getName();
            byte[] value = null;

            try
            {
                value = PropertyAccessorHelper.get(superColumnObject, field);
            }
            catch (PropertyAccessException exp)
            {
                // This is an entity column to be persisted in a super column
                // family. It will be stored as a super column that would
                // have just one column with the same name
                log
                        .info(exp.getMessage()
                                + ". Possible case of entity column in a super column family. Will be treated as a super column.");
                value = superColumnObject.toString().getBytes();
            }

            if (null != value)
            {
                Column thriftColumn = new Column();

                thriftColumn.setName(PropertyAccessorFactory.STRING.toBytes(name));
                thriftColumn.setValue(value);
                thriftColumn.setTimestamp(timestamp);
                thriftColumns.add(thriftColumn);
            }
        }
        SuperColumn thriftSuperColumn = new SuperColumn();

        thriftSuperColumn.setName(PropertyAccessorFactory.STRING.toBytes(superColumnName));
        thriftSuperColumn.setColumns(thriftColumns);

        return thriftSuperColumn;
    }

    /**
     * Gets the foreign keys from join table.
     *
     * @param <E> the element type
     * @param inverseJoinColumnName the inverse join column name
     * @param columns the columns
     * @return the foreign keys from join table
     */
    public <E> List<E> getForeignKeysFromJoinTable(String inverseJoinColumnName, List<Column> columns)
    {
        List<E> foreignKeys = new ArrayList<E>();

        if (columns == null || columns.isEmpty())
        {
            return foreignKeys;
        }

        for (Column c : columns)
        {
            try
            {
                // Thrift Column name
                String thriftColumnName = PropertyAccessorFactory.STRING.fromBytes(c.getName());

                // Thrift Column Value
                byte[] thriftColumnValue = c.getValue();
                if (null == thriftColumnValue)
                {
                    continue;
                }

                if (thriftColumnName != null && thriftColumnName.startsWith(inverseJoinColumnName))
                {
                    String val = PropertyAccessorFactory.STRING.fromBytes(thriftColumnValue);
                    foreignKeys.add((E) val);
                }
            }
            catch (PropertyAccessException e)
            {
                continue;
            }

        }
        return foreignKeys;
    }

    /**
     * Gets the timestamp.
     *
     * @return the timestamp
     */
    public long getTimestamp()
    {
        return timestamp;
    }
}
