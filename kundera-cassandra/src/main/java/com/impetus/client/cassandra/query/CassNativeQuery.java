/**
 * *****************************************************************************
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); * you may
 * not use this file except in compliance with the License. * You may obtain a
 * copy of the License at * * http://www.apache.org/licenses/LICENSE-2.0 * *
 * Unless required by applicable law or agreed to in writing, software *
 * distributed under the License is distributed on an "AS IS" BASIS, * WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. * See the
 * License for the specific language governing permissions and * limitations
 * under the License.
 * ****************************************************************************
 */
package com.impetus.client.cassandra.query;

import com.impetus.client.cassandra.pelops.ByteUtils;
import com.impetus.client.cassandra.pelops.CassandraDataHandler;
import com.impetus.client.cassandra.pelops.ThriftRow;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.QueryImpl;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.persistence.Query;
import org.apache.cassandra.thrift.*;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author kcarlson
 */
public class CassNativeQuery extends QueryImpl implements Query
{

    private static Log log = LogFactory.getLog(CassNativeQuery.class);

    private EntityReader reader;

    private int maxResult = 10000;

    public static Compression defaultCompression = Compression.GZIP;

    private final CassandraDataHandler dataHandler;

    private final String[] persistenceUnits;

    private List currentResultList;

    private int updateCount;

    private String currentColumnFamily;

    public CassNativeQuery(String query, KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator,
            String[] persistenceUnits)
    {
        super(query, persistenceDelegator, persistenceUnits);
        this.kunderaQuery = (KunderaQuery) kunderaQuery;
        this.dataHandler = new CassandraDataHandler();
        this.persistenceUnits = persistenceUnits;
        currentResultList = new ArrayList();
        updateCount = 0;
    }

    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client)
    {
        /*
         * log.debug("on populateEntities cassandra query"); List<Object> result
         * = null; if (MetadataUtils.useSecondryIndex(m.getPersistenceUnit())) {
         *
         * Map<Boolean, List<IndexClause>> ixClause = prepareIndexClause(m);
         * boolean isRowKeyQuery = ixClause.keySet().iterator().next(); if
         * (!isRowKeyQuery) { result = ((PelopsClient)
         * client).find(ixClause.get(isRowKeyQuery), m, false, null); } else {
         * result = ((CassandraEntityReader) getReader()).handleFindByRange(m,
         * client, result, ixClause, isRowKeyQuery); } } else { result =
         * populateUsingLucene(m, client, result);
         *
         * }
         * return result;
         */

        throw new NotImplementedException("TODO");
    }

    @Override
    protected List<Object> handleAssociations(EntityMetadata m, Client client, List<EntitySaveGraph> graphs,
            List<String> relationNames, boolean isParent)
    {
        /*
         * log.debug("on handleAssociations cassandra query"); Map<Boolean,
         * List<IndexClause>> ixClause = prepareIndexClause(m);
         *
         * ((CassandraEntityReader) getReader()).setConditions(ixClause);
         *
         * List<EnhanceEntity> ls = reader.populateRelation(m, relationNames,
         * isParent, client);
         *
         * return handleGraph(ls, graphs, client, m);
         */
        throw new NotImplementedException("TODO");
    }

    @Override
    protected EntityReader getReader()
    {
        /*
         * if (reader == null) { reader = new
         * CassandraEntityReader(getLuceneQueryFromJPAQuery()); }
         *
         * return reader;
         */
        throw new NotImplementedException("TODO");
    }

    @Override
    public int executeUpdate()
    {
        doExecute();

        return updateCount;
    }

    @Override
    public Query setFirstResult(int startPosition)
    {
        throw new NotImplementedException("TODO");
    }

    @Override
    public Object getSingleResult()
    {
        doExecute();

        if (currentResultList.size() > 1)
        {
            throw new RuntimeException("Query returned more than one result: " + query);
        }

        return currentResultList.isEmpty() ? null : currentResultList.get(0);
    }

    @Override
    public Query setMaxResults(int maxResult)
    {
        this.maxResult = maxResult;
        return this;
    }

    @Override
    public List<?> getResultList()
    {
        doExecute();

        return currentResultList != null && !currentResultList.isEmpty() ? currentResultList : null;
    }

    private void doExecute()
    {
        try
        {
            log.info("On getResultList() executing query: " + query);
            CassandraCqlClient client = CassandraCqlClient.getInstance();
            currentColumnFamily = client.currentColumnFamily;
            CqlResult result = client.executeQuery(query);

            switch (result.getType())
            {
            case ROWS:
                populateCurrentResultList(result);
                break;
            case INT:
                updateCount = result.getNum();
                break;
            case VOID:
                updateCount = 0;
                break;
            }

            /*
             * try { EntityMetadata m = kunderaQuery.getEntityMetadata(); Client
             * client = persistenceDelegeator.getClient(m);
             *
             * // get Graph List<EntitySaveGraph> graphs =
             * persistenceDelegeator.getGraph(m.getEntityClazz().newInstance(),
             * m); // Get relations. Map<Boolean, List<String>> relationHolder =
             * persistenceDelegeator.getRelations(graphs, m.getEntityClazz());
             * List<String> relationNames =
             * relationHolder.values().iterator().next(); boolean isParent =
             * relationHolder.keySet().iterator().next();
             *
             * if (relationNames.isEmpty() && !m.isRelationViaJoinTable()) {
             *
             * // There is no association so simply return list of entities.
             * results = populateEntities(m, client); } else { results =
             * handleAssociations(m, client, graphs, relationNames, isParent); }
             *
             * }
             * catch (InstantiationException e) { log.error("error while
             * returing query result:" + e.getMessage()); throw new
             * QueryHandlerException(e.getMessage()); } catch
             * (IllegalAccessException e) { log.error("error while returing
             * query result:" + e.getMessage()); throw new
             * QueryHandlerException(e.getMessage()); } // Query is parsed. //
             * get Graph // If there is any relation and entity is not parent,
             * // get client from persistenceDelegator and find that object. //
             * set that object in graph // Populate child entities according to
             * graph. // if entity is parent pass it as foreign key id for
             * client // if entity is not parent then pass retrieved relation
             * key value to // specific client for find by id.
             *
             */

        }
        catch (Exception ex)
        {
            log.fatal(null, ex);
        }
    }

    private void populateCurrentResultList(CqlResult result)
    {
        try
        {
            currentResultList = new ArrayList();

            Iterator<CqlRow> it = result.getRowsIterator();

            while (it.hasNext())
            {

                CqlRow cqlRow = it.next();
                String rowKey = ByteUtils.byteArrayToString(cqlRow.getKey());

                ThriftRow thriftRow = new ThriftRow(rowKey, currentColumnFamily, cqlRow.getColumns(), null);

                Object entity = dataHandler.fromColumnThriftRow(kunderaQuery.getEntityClass(), kunderaQuery
                        .getEntityMetadata(), thriftRow, null, false);

                currentResultList.add(entity);

            }
        }
        catch (Exception ex)
        {
            log.fatal(null, ex);
        }
    }
}
