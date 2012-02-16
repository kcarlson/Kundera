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
package com.impetus.kundera.cache.ehcache;

import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.query.KunderaJpaQuery;
import com.impetus.kundera.query.KunderaJpaQuery.SortOrder;
import com.impetus.kundera.query.KunderaJpaQuery.SortOrdering;
import com.impetus.kundera.query.KunderaJpaQueryParser;
import com.impetus.kundera.query.KunderaQueryParserException;

/**
 * The Class KunderaQueryParserTest.
 *
 * @author vivek.mishra
 */
public class KunderaQueryParserTest
{

    /**
     * Sets the up.
     *
     * @throws Exception the exception
     */
    @Before
    public void setUp() throws Exception
    {
    }

    /**
     * On query parse.
     */
    @Test
    public void onQueryParse()
    {
        KunderaJpaQuery kunderQuery = new KunderaJpaQuery("rdbms");

        // Valid Query with clause.
        String validQuery = "SELECT c FROM Country c ORDER BY c.currency, c.population DESC";

        KunderaJpaQueryParser parser = new KunderaJpaQueryParser(kunderQuery, validQuery);
        parser.parse();

        List<SortOrdering> sortOrders = kunderQuery.getOrdering();
        Assert.assertNotNull(sortOrders);
        Assert.assertEquals(2, sortOrders.size());
        Assert.assertEquals("c.currency", sortOrders.get(0).getColumnName());
        Assert.assertEquals(SortOrder.ASC.name(), sortOrders.get(0).getOrder().name());
        Assert.assertEquals("c.population", sortOrders.get(1).getColumnName());
        Assert.assertEquals(SortOrder.DESC.name(), sortOrders.get(1).getOrder().name());

        // valid query with default ASC clause.
        String validQueryWithDefaultClause = "SELECT c FROM Country c ORDER BY c.currency, c.population";

        parser = new KunderaJpaQueryParser(kunderQuery, validQueryWithDefaultClause);
        parser.parse();

        sortOrders = kunderQuery.getOrdering();
        Assert.assertNotNull(sortOrders);
        Assert.assertEquals(2, sortOrders.size());
        Assert.assertEquals("c.currency", sortOrders.get(0).getColumnName());
        Assert.assertEquals(SortOrder.ASC.name(), sortOrders.get(0).getOrder().name());
        Assert.assertEquals("c.population", sortOrders.get(1).getColumnName());
        Assert.assertEquals(SortOrder.ASC.name(), sortOrders.get(1).getOrder().name());

        String invalidQuery = "SELECT c FROM Country c ORDER BY c.currency, c.population DESCS";

        try
        {
            parser = new KunderaJpaQueryParser(kunderQuery, validQueryWithDefaultClause);
            parser.parse();
        }
        catch (KunderaQueryParserException e)
        {
            Assert.assertNotNull(e);
        }
    }

    /**
     * Tear down.
     *
     * @throws Exception the exception
     */
    @After
    public void tearDown() throws Exception
    {

    }

}
