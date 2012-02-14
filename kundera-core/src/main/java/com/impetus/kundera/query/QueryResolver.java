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
package com.impetus.kundera.query;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Properties;

import javax.persistence.Query;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.ClientType;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;


/**
 * The Class QueryResolver.
 * 
 * @author amresh.singh
 * 
 */
public class QueryResolver
{

    /** The log. */
    private static Log log = LogFactory.getLog(QueryResolver.class);

    /** The kundera query. */
    KunderaQuery kunderaQuery;

    public Query getNativeQueryImplementation(String cqlQuery, PersistenceDelegator persistenceDelegator, String... persistenceUnits)
    {
        return getQueryImplementation(cqlQuery, persistenceDelegator, true, persistenceUnits);
    }
    
    /**
     * Gets the query implementation.
     * 
     * @param jpaQuery
     *            the jpa query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param persistenceUnits
     *            the persistence units
     * @return the query implementation
     */
    public Query getQueryImplementation(String jpaQuery, PersistenceDelegator persistenceDelegator,
            String... persistenceUnits)
    {
        return getQueryImplementation(jpaQuery, persistenceDelegator, false, persistenceUnits);
    }
    
    private Query getQueryImplementation(String query, PersistenceDelegator persistenceDelegator, boolean isNative, String... persistenceUnits)
    {
        String pu = null;
        
        if (persistenceUnits.length == 1)
        {
            pu = persistenceUnits[0];
        }
        
        if (StringUtils.isEmpty(pu))
        {
            Map<String, PersistenceUnitMetadata> puMetadataMap = KunderaMetadata.INSTANCE.getApplicationMetadata()
                    .getPersistenceUnitMetadataMap();
            for (PersistenceUnitMetadata puMetadata : puMetadataMap.values())
            {
                Properties props = puMetadata.getProperties();
                String clientName = props.getProperty(PersistenceProperties.KUNDERA_CLIENT);
                if (ClientType.RDBMS.name().equalsIgnoreCase(clientName))
                {
                    pu = puMetadata.getPersistenceUnitName();
                    break;
                }

            }
        }

        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(pu);
        String kunderaClientName = (String) puMetadata.getProperties().get(PersistenceProperties.KUNDERA_CLIENT);
        ClientType clientType = ClientType.valueOf(kunderaClientName.toUpperCase());
        clientType.setNative(isNative);
        
        KunderaQueryFactory kunderaQueryFactory = new KunderaQueryFactory(clientType);
        
        kunderaQuery = kunderaQueryFactory.build(query, persistenceUnits);
        
        
        Query q = null;

        try
        {
            q = getQuery(clientType, query, persistenceDelegator, persistenceUnits);
        }
        catch (SecurityException e)
        {
            log.error(e.getMessage());
        }
        catch (IllegalArgumentException e)
        {
            log.error(e.getMessage());
        }
        catch (ClassNotFoundException e)
        {
            log.error(e.getMessage());
        }
        catch (NoSuchMethodException e)
        {
            log.error(e.getMessage());
        }
        catch (InstantiationException e)
        {
            log.error(e.getMessage());
        }
        catch (IllegalAccessException e)
        {
            log.error(e.getMessage());
        }
        catch (InvocationTargetException e)
        {
            log.error(e.getMessage());
        }

        return q;

    }

    /**
     * Gets the query.
     * 
     * @param clientType
     *            the client type
     * @param jpaQuery
     *            the jpa query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param persistenceUnits
     *            the persistence units
     * @return the query
     * @throws ClassNotFoundException
     *             the class not found exception
     * @throws SecurityException
     *             the security exception
     * @throws NoSuchMethodException
     *             the no such method exception
     * @throws IllegalArgumentException
     *             the illegal argument exception
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     * @throws InvocationTargetException
     *             the invocation target exception
     */
    public Query getQuery(ClientType clientType, String jpaQuery, PersistenceDelegator persistenceDelegator,
            String... persistenceUnits) throws ClassNotFoundException, SecurityException, NoSuchMethodException,
            IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException
    {
        Query query;
        Class clazz = null;
        switch (clientType)
        {
            case HBASE:
                clazz = Class.forName("com.impetus.kundera.query.LuceneQuery");
                break;
            case MONGODB:
                clazz = Class.forName("com.impetus.client.mongodb.query.MongoDBQuery");
                break;
            case PELOPS:
                if(clientType.isNative())
                {
                    clazz = Class.forName("com.impetus.client.cassandra.query.CassNativeQuery");
                }
                else
                {
                    clazz = Class.forName("com.impetus.client.cassandra.query.CassQuery");
                }
                break;
            case THRIFT:
                clazz = Class.forName("com.impetus.client.cassandra.query.CassQuery");
                break;
            case RDBMS:
                clazz = Class.forName("com.impetus.client.rdbms.query.RDBMSQuery");
                break;
            default:
                throw new ClassNotFoundException("Invalid Client type" + clientType);
                // break;
        }

        @SuppressWarnings("rawtypes")
        Constructor constructor = clazz.getConstructor(String.class, KunderaJpaQuery.class, PersistenceDelegator.class,
                String[].class);
        query = (Query) constructor.newInstance(jpaQuery, kunderaQuery, persistenceDelegator, persistenceUnits);

        return query;

    }

}
