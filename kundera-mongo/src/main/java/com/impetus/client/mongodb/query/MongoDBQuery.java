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
package com.impetus.client.mongodb.query;

import javax.persistence.Query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.QueryImpl;

/**
 * Query class for MongoDB data store
 * 
 * @author amresh.singh
 */
public class MongoDBQuery extends QueryImpl
{
    /** The log used by this class. */
    private static Log log = LogFactory.getLog(MongoDBQuery.class);

    public MongoDBQuery(String jpaQuery, KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator,
            String... persistenceUnits)
    {
        super(jpaQuery, persistenceDelegator, persistenceUnits);
        this.kunderaQuery = kunderaQuery;
    }

    /*
     * @Override public List<?> getResultList() { log.debug("JPA Query is: " +
     * query);
     * 
     * EntityMetadata m = getEntityMetadata();
     * 
     * try { return getClient().loadData(this); } catch (Exception e) { return
     * null; } }
     */

    @Override
    public int executeUpdate()
    {
        return super.executeUpdate();
    }

    @Override
    public Query setMaxResults(int maxResult)
    {
        return super.setMaxResults(maxResult);
    }

}
