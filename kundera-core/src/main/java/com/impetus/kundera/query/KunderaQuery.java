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

import java.util.List;
import java.util.Queue;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.query.KunderaJpaQuery.SortOrdering;
import java.util.Set;
import javax.persistence.Parameter;

/**
 * The Class KunderaQuery.
 */
public interface KunderaQuery
{

    /**
     * Sets the grouping.
     * 
     * @param groupingClause
     *            the new grouping
     */
    public void setGrouping(String groupingClause);

    /**
     * Sets the result.
     * 
     * @param result
     *            the new result
     */
    public void setResult(String result);

    /**
     * Sets the from.
     * 
     * @param from
     *            the new from
     */
    public void setFrom(String from);

    /**
     * Sets the filter.
     * 
     * @param filter
     *            the new filter
     */
    public void setFilter(String filter);

    /**
     * Sets the ordering.
     * 
     * @param ordering
     *            the new ordering
     */
    public void setOrdering(String ordering);

    /**
     * Gets the filter.
     * 
     * @return the filter
     */
    public String getFilter();

    /**
     * Gets the from.
     * 
     * @return the from
     */
    public String getFrom();

    /**
     * Gets the ordering.
     * 
     * @return the ordering
     */
    public List<SortOrdering> getOrdering();

    /**
     * Gets the result.
     * 
     * @return the result
     */
    public String getResult();

    /**
     * Method to check if required result is to get complete entity or a select
     * scalar value.
     * 
     * @return true, if it result is for complete alias.
     * 
     */
    public boolean isAliasOnly();

    // must be executed after parse(). it verifies and populated the query
    // predicates.
    /**
     * Post parsing init.
     */
    public void postParsingInit();

    /**
     * Sets the parameter.
     * 
     * @param name
     *            the name
     * @param value
     *            the value
     */
    public void setParameter(String name, String value);

    /**
     * Gets the entity class.
     * 
     * @return the entityClass
     */
    public Class getEntityClass();

    /**
     * Gets the entity metadata.
     * 
     * @return the entity metadata
     */
    public EntityMetadata getEntityMetadata();

    /**
     * Gets the filter clause queue.
     * 
     * @return the filters
     */
    public Queue getFilterClauseQueue();

    /**
     * Gets the persistence units.
     * 
     * @return the persistenceUnits
     */
    public String[] getPersistenceUnits();

    /**
     * Sets the persistence units.
     * 
     * @param persistenceUnits
     *            the persistenceUnits to set
     */
    public void setPersistenceUnits(String[] persistenceUnits);

    public Set<Parameter<?>> getParameters();

    public Parameter<?> getParameter(String paramString);

    public Object getParameterValue(String paramString);

}
