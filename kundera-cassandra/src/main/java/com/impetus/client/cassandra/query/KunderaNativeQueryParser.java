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

import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQueryParser;

/**
 * Parses the CQL for data needed to build entities.
 * @author kcarlson
 */
public class KunderaNativeQueryParser implements KunderaQueryParser
{
    private KunderaNativeQuery query;
    private String queryString;

    /**
     * Constructor for the Single-String parser.
     * 
     * @param query
     *            The query
     * @param queryString
     *            The Single-String query
     */
    public KunderaNativeQueryParser(KunderaQuery query, String queryString)
    {
        this.query = (KunderaNativeQuery)query;
        this.queryString = queryString;
    }
    
    @Override
    public void parse()
    {
        String columnFamily = CqlUtils.determineCurrentColumnFamily(queryString);
        query.setFrom(columnFamily);
    }
    
}
