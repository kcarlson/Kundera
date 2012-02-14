/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.impetus.client.cassandra.query;

import com.impetus.kundera.query.KunderaJpaQuery;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQueryParser;

/**
 *
 * @author kcarlson
 */
public class KunderaNativeQueryParser implements KunderaQueryParser
{
    private KunderaJpaQuery query;
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
        this.query = (KunderaJpaQuery)query;
        this.queryString = queryString;
    }
    
    @Override
    public void parse()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
