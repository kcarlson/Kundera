/*******************************************************************************
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

package com.impetus.client.cassandra.query;

import com.impetus.client.cassandra.pelops.PelopsClientFactory;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.TException;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.pool.IThriftPool;

import static com.impetus.client.cassandra.query.CqlUtils.*;
import java.sql.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author kcarlson
 */
public class CassandraCqlClient
{
    private static Log log = LogFactory.getLog(CassNativeQuery.class);

    private final Client client;

    protected long timeOfLastFailure = 0;

    protected int numFailures = 0;

    protected String username = null;

    protected String url = null;

    String currentKeyspace;

    //ColumnDecoder decoder;
    private Compression defaultCompression = Compression.GZIP;

    String currentColumnFamily;

    private static class CassandraCqlClientHolder
    {
        static final CassandraCqlClient INSTANCE = new CassandraCqlClient();
    }

    static CassandraCqlClient getInstance()
    {
        return CassandraCqlClientHolder.INSTANCE;
    }

    private CassandraCqlClient()
    {
        if (PelopsClientFactory.POOL_NAME == null)
        {
            //TODO: Cleanup.
            throw new RuntimeException("Pool name is null");
        }
        IThriftPool dbConnPool = Pelops.getDbConnPool(PelopsClientFactory.POOL_NAME);
        IThriftPool.IPooledConnection connection = dbConnPool.getConnection();
        client = connection.getAPI();

        try
        {
            String keyspace = dbConnPool.getKeyspace();
            if (keyspace != null)
            {
                execute("USE " + keyspace);
            }
        }
        catch (SchemaDisagreementException e)
        {
            log.fatal(null, e);
        }
        catch (InvalidRequestException e)
        {
            log.fatal(null, e);
        }
        catch (UnavailableException e)
        {
            log.fatal(null, e);
        }
        catch (TimedOutException e)
        {
            log.fatal(null, e);
        }
        catch (TException e)
        {
            log.fatal(null, e);
        }
    }

    /**
     * Execute a CQL query using the default compression methodology.
     *
     * @param queryStr a CQL query string
     * @return the query results encoded as a CqlResult structure
     * @throws InvalidRequestException     on poorly constructed or illegal requests
     * @throws UnavailableException        when not all required replicas could be created/read
     * @throws TimedOutException           when a cluster operation timed out
     * @throws SchemaDisagreementException when the client side and server side are at different versions of schema (Thrift)
     * @throws TException                  when there is a error in Thrift processing
     */
    protected CqlResult execute(String queryStr) throws InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException, TException
    {
        return execute(queryStr, defaultCompression);
    }

    /**
     * Execute a CQL query.
     *
     * @param queryStr    a CQL query string
     * @param compression query compression to use
     * @return the query results encoded as a CqlResult structure
     * @throws InvalidRequestException     on poorly constructed or illegal requests
     * @throws UnavailableException        when not all required replicas could be created/read
     * @throws TimedOutException           when a cluster operation timed out
     * @throws SchemaDisagreementException when the client side and server side are at different versions of schema (Thrift)
     * @throws TException                  when there is a error in Thrift processing
     */
    protected CqlResult execute(String queryStr, Compression compression) throws InvalidRequestException,
            UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        currentKeyspace = determineCurrentKeyspace(queryStr, currentKeyspace);
        currentColumnFamily = determineCurrentColumnFamily(queryStr);

        try
        {
            return client.execute_cql_query(compressQuery(queryStr, compression), compression);
        }
        catch (TException error)
        {
            numFailures++;
            timeOfLastFailure = System.currentTimeMillis();
            throw error;
        }
    }

    public CqlResult executeQuery(String query) throws InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException, TException
    {
        //TODO: Check not closed.
        return execute(query, defaultCompression);
    }
}
