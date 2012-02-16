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
package com.impetus.client.mongodb;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.persistence.Embedded;
import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EmbeddedColumn;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * Provides utility methods for handling data held in MongoDB.
 *
 * @author amresh.singh
 */
public class MongoDBDataHandler
{

    /** The client. */
    private Client client;

    /** The persistence unit. */
    private String persistenceUnit;

    /**
     * Instantiates a new mongo db data handler.
     *
     * @param client the client
     * @param persistenceUnit the persistence unit
     */
    public MongoDBDataHandler(Client client, String persistenceUnit)
    {
        super();
        this.client = client;
        this.persistenceUnit = persistenceUnit;
    }

    /** The log. */
    private static Log log = LogFactory.getLog(MongoDBDataHandler.class);

    /**
     * Gets the entity from document.
     *
     * @param entityClass the entity class
     * @param m the m
     * @param document the document
     * @param relations the relations
     * @return the entity from document
     */
    public Object getEntityFromDocument(Class<?> entityClass, EntityMetadata m, DBObject document,
            List<String> relations)
    {
        // Entity object
        Object entity = null;

        // Map to hold property-name=>foreign-entity relations
        // Map<String, Set<String>> foreignKeysMap = new HashMap<String,
        // Set<String>>();

        try
        {
            entity = entityClass.newInstance();

            // Populate primary key column
            String rowKey = (String) document.get("_id");
            PropertyAccessorHelper.setId(entity, m, rowKey);

            // Populate entity columns
            List<Column> columns = m.getColumnsAsList();
            for (Column column : columns)
            {
                PropertyAccessorHelper.set(entity, column.getField(), document.get(column.getName()).toString());
            }

            // Populate @Embedded objects and collections
            List<EmbeddedColumn> embeddedColumns = m.getEmbeddedColumnsAsList();
            for (EmbeddedColumn embeddedColumn : embeddedColumns)
            {
                Field embeddedColumnField = embeddedColumn.getField();
                // Can be a BasicDBObject or a list of it.
                Object embeddedDocumentObject = document.get(embeddedColumnField.getName());

                if (embeddedDocumentObject != null)
                {
                    if (embeddedDocumentObject instanceof BasicDBList)
                    {
                        Class embeddedObjectClass = PropertyAccessorHelper.getGenericClass(embeddedColumnField);
                        Collection embeddedCollection = DocumentObjectMapper.getCollectionFromDocumentList(
                                (BasicDBList) embeddedDocumentObject, embeddedColumnField.getType(),
                                embeddedObjectClass, embeddedColumn.getColumns());
                        PropertyAccessorHelper.set(entity, embeddedColumnField, embeddedCollection);
                    }
                    else if (embeddedDocumentObject instanceof BasicDBObject)
                    {
                        Object embeddedObject = null;
                        if (embeddedColumnField.isAnnotationPresent(Embedded.class))
                        {
                            embeddedObject = DocumentObjectMapper.getObjectFromDocument(
                                    (BasicDBObject) embeddedDocumentObject, embeddedColumn.getField().getType(),
                                    embeddedColumn.getColumns());
                        }
                        else
                        {

                            embeddedObject = ((BasicDBObject) embeddedDocumentObject).get(embeddedColumn.getName());

                        }
                        PropertyAccessorHelper.set(entity, embeddedColumnField, embeddedObject);

                    }
                    else
                    {
                        throw new PersistenceException("Can't retrieve embedded object from MONGODB document coz "
                                + "it wasn't stored as BasicDBObject, possible problem in format.");
                    }
                }

            }

            if (relations != null)
            {
                EnhanceEntity e = null;
                Map<String, Object> relationValue = new HashMap<String, Object>();
                for (String r : relations)
                {
                    if (relationValue == null)
                    {
                        relationValue = new HashMap<String, Object>();
                    }
                    Object colValue = document.get(r);
                    relationValue.put(r, colValue);
                }

                e = new EnhanceEntity(entity, PropertyAccessorHelper.getId(entity, m), relationValue);
                return e;
            }
            return entity;

        }
        catch (InstantiationException e)
        {
            log.error("Error while instantiating " + entityClass + ". Details:" + e.getMessage());
            return entity;
        }
        catch (IllegalAccessException e)
        {
            log.error("Error while Getting entity from Document. Details:" + e.getMessage());
            return entity;
        }
        catch (PropertyAccessException e)
        {
            log.error("Error while Getting entity from Document. Details:" + e.getMessage());
            return entity;
        }

    }

    /**
     * Gets the client.
     *
     * @return the client
     */
    private Client getClient()
    {
        return client;
    }

    /**
     * Gets the persistence unit.
     *
     * @return the persistence unit
     */
    private String getPersistenceUnit()
    {
        return persistenceUnit;
    }

    /**
     * Gets the document from entity.
     *
     * @param m the m
     * @param entity the entity
     * @param relations the relations
     * @return the document from entity
     * @throws PropertyAccessException the property access exception
     */
    public BasicDBObject getDocumentFromEntity(EntityMetadata m, Object entity, List<RelationHolder> relations)
            throws PropertyAccessException
    {
        List<Column> columns = m.getColumnsAsList();
        BasicDBObject dbObj = new BasicDBObject();

        // Populate Row Key
        dbObj.put("_id", PropertyAccessorHelper.getId(entity, m));
        dbObj.put(m.getIdColumn().getName(), PropertyAccessorHelper.getId(entity, m));

        // Populate columns
        for (Column column : columns)
        {
            try
            {
                extractEntityField(entity, dbObj, column);
            }
            catch (PropertyAccessException e1)
            {
                log.error("Can't access property " + column.getField().getName());
            }
        }

        // Populate @Embedded objects and collections
        List<EmbeddedColumn> embeddedColumns = m.getEmbeddedColumnsAsList();
        for (EmbeddedColumn embeddedColumn : embeddedColumns)
        {
            Field superColumnField = embeddedColumn.getField();
            Object embeddedObject = PropertyAccessorHelper.getObject(entity, superColumnField);

            if (embeddedObject != null)
            {
                if (embeddedObject instanceof Collection)
                {

                    Collection embeddedCollection = (Collection) embeddedObject;

                    dbObj.put(superColumnField.getName(), DocumentObjectMapper.getDocumentListFromCollection(
                            embeddedCollection, embeddedColumn.getColumns()));
                }
                else
                {
                    if (superColumnField.isAnnotationPresent(Embedded.class))
                    {
                        dbObj.put(superColumnField.getName(), DocumentObjectMapper.getDocumentFromObject(
                                embeddedObject, embeddedColumn.getColumns()));
                    }
                    else
                    {
                        dbObj.put(superColumnField.getName(), DocumentObjectMapper.getDocumentFromObject(entity,
                                embeddedColumn.getColumns()));

                    }
                }
            }
        }

        // Populate foreign keys
        if (relations != null)
        {
            for (RelationHolder rh : relations)
            {
                dbObj.put(rh.getRelationName(), rh.getRelationValue());
            }
        }

        return dbObj;
    }

    /**
     * Extract entity field.
     *
     * @param entity the entity
     * @param dbObj the db obj
     * @param column the column
     * @throws PropertyAccessException the property access exception
     */
    private void extractEntityField(Object entity, BasicDBObject dbObj, Column column) throws PropertyAccessException
    {
        // A column field may be a collection(not defined as 1-to-M
        // relationship)
        if (column.getField().getType().equals(List.class) || column.getField().getType().equals(Set.class))
        {
            Collection collection = (Collection) PropertyAccessorHelper.getObject(entity, column.getField());
            BasicDBList basicDBList = new BasicDBList();
            for (Object o : collection)
            {
                basicDBList.add(o);
            }
            dbObj.put(column.getName(), basicDBList);
        }
        else
        {
            // TODO : this should have been handled by DocumentObjectMapper.
            Object valObj = PropertyAccessorHelper.getObject(entity, column.getField());
            dbObj.put(column.getName(), valObj instanceof Calendar ? ((Calendar) valObj).getTime().toString()
                    : PropertyAccessorHelper.getObject(entity, column.getField()).toString());
        }
    }

    /**
     * Returns column name from the filter property which is in the form
     * dbName.columnName
     *
     * @param filterProperty the filter property
     * @return the column name
     */
    public String getColumnName(String filterProperty)
    {
        StringTokenizer st = new StringTokenizer(filterProperty, ".");
        String columnName = "";
        while (st.hasMoreTokens())
        {
            columnName = st.nextToken();
        }

        return columnName;
    }

    /**
     * Gets the enclosing document name.
     *
     * @param m the m
     * @param columnName the column name
     * @return the enclosing document name
     */
    public String getEnclosingDocumentName(EntityMetadata m, String columnName)
    {
        String enclosingDocumentName = null;
        if (!m.getColumnFieldNames().contains(columnName))
        {

            for (EmbeddedColumn superColumn : m.getEmbeddedColumnsAsList())
            {
                List<Column> columns = superColumn.getColumns();
                for (Column column : columns)
                {
                    if (column.getName().equals(columnName))
                    {
                        enclosingDocumentName = superColumn.getName();
                        break;
                    }
                }
            }

        }
        return enclosingDocumentName;
    }

    /**
     * Retrieves A collection of embedded object within a document that match a
     * criteria specified in <code>query</code> TODO: This code requires a
     * serious overhawl. Currently it assumes that user query is in the form
     * "Select alias.columnName from EntityName alias". However, correct query
     * to be supported is
     * "Select alias.superColumnName.columnName from EntityName alias"
     *
     * @param dbCollection the db collection
     * @param m the m
     * @param documentName the document name
     * @param mongoQuery the mongo query
     * @param result the result
     * @param orderBy the order by
     * @return the embedded object list
     * @throws PropertyAccessException the property access exception
     */
    public List getEmbeddedObjectList(DBCollection dbCollection, EntityMetadata m, String documentName,
            BasicDBObject mongoQuery, String result, BasicDBObject orderBy) throws PropertyAccessException
    {
        List list = new ArrayList();// List of embedded object to be returned

        // MongoDBQuery mongoDBQuery = (MongoDBQuery) query;

        // Specified after entity alias in query
        String columnName = getColumnName(result);

        // Something user didn't specify and we have to derive
        // TODO: User must specify this in query and remove this logic once
        // query format is changed
        String enclosingDocumentName = getEnclosingDocumentName(m, columnName);

        // Query for fetching entities based on user specified criteria
        DBCursor cursor = orderBy != null ? dbCollection.find(mongoQuery).sort(orderBy) : dbCollection.find(mongoQuery);

        EmbeddedColumn superColumn = m.getEmbeddedColumn(enclosingDocumentName);
        Field superColumnField = superColumn.getField();
        while (cursor.hasNext())
        {
            DBObject fetchedDocument = cursor.next();
            Object embeddedDocumentObject = fetchedDocument.get(superColumnField.getName());

            if (embeddedDocumentObject != null)
            {
                if (embeddedDocumentObject instanceof BasicDBList)
                {
                    Class embeddedObjectClass = PropertyAccessorHelper.getGenericClass(superColumnField);
                    for (Object dbObj : (BasicDBList) embeddedDocumentObject)
                    {
                        Object embeddedObject = new DocumentObjectMapper().getObjectFromDocument((BasicDBObject) dbObj,
                                embeddedObjectClass, superColumn.getColumns());
                        Object fieldValue = PropertyAccessorHelper.getObject(embeddedObject, columnName);

                        // TODO : discussion required with amresh on this.
                        /*
                         * for (Object object : filterClauseQueue) { if (object
                         * instanceof FilterClause) { FilterClause filter =
                         * (FilterClause) object; String value =
                         * filter.getValue(); String condition =
                         * filter.getCondition();
                         * 
                         * // This is not an ideal and complete //
                         * implementation. A similar logic exists in //
                         * createMongoQuery method. Need to find a way // to
                         * combine them if (condition.equals("=")) { if
                         * (value.equals(fieldValue)) {
                         * list.add(embeddedObject); } } else if
                         * (condition.equalsIgnoreCase("like")) { if
                         * (fieldValue.toString().indexOf(value) >= 0) {
                         * list.add(embeddedObject); } }
                         * 
                         * } }
                         */
                    }

                }
                else if (embeddedDocumentObject instanceof BasicDBObject)
                {
                    Object embeddedObject = DocumentObjectMapper.getObjectFromDocument(
                            (BasicDBObject) embeddedDocumentObject, superColumn.getField().getType(), superColumn
                                    .getColumns());
                    list.add(embeddedObject);

                }
                else
                {
                    throw new PersistenceException("Can't retrieve embedded object from MONGODB document coz "
                            + "it wasn't stored as BasicDBObject, possible problem in format.");
                }
            }

        }
        return list;
    }
}