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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.PersistenceException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;

/**
 * The Class KunderaQuery.
 */
public class KunderaJpaQuery implements KunderaQuery
{

    /** The Constant SINGLE_STRING_KEYWORDS. */
    public static final String[] SINGLE_STRING_KEYWORDS = { "SELECT", "UPDATE", "DELETE", "UNIQUE", "FROM", "WHERE",
            "GROUP BY", "HAVING", "ORDER BY" };

    /** The Constant INTER_CLAUSE_OPERATORS. */
    public static final String[] INTER_CLAUSE_OPERATORS = { "AND", "OR", "BETWEEN" };

    /** The Constant INTRA_CLAUSE_OPERATORS. */
    public static final String[] INTRA_CLAUSE_OPERATORS = { "=", "LIKE", ">", ">=", "<", "<=" };

    /** The INTER pattern. */
    private static final Pattern INTER_CLAUSE_PATTERN = Pattern.compile("\\band\\b|\\bor\\b|\\bbetween\\b",
            Pattern.CASE_INSENSITIVE);

    /** The INTRA pattern. */
    private static final Pattern INTRA_CLAUSE_PATTERN = Pattern.compile("=|\\blike\\b|>=|>|<=|<",
            Pattern.CASE_INSENSITIVE);

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(KunderaJpaQuery.class);

    /** The result. */
    private String result;

    /** The from. */
    private String from;

    /** The filter. */
    private String filter;

    /** The ordering. */
    private String ordering;

    /** The entity name. */
    private String entityName;

    /** The entity alias. */
    private String entityAlias;

    /** The entity class. */
    private Class<?> entityClass;

    /** The sort orders. */
    private List<SortOrdering> sortOrders;

    /** Persistence Unit(s). */
    String[] persistenceUnits;

    // contains a Queue of alternate FilterClause object and Logical Strings
    // (AND, OR etc.)
    /** The filters queue. */
    private Queue filtersQueue = new LinkedList();

    /**
     * Instantiates a new kundera query.
     * 
     * @param persistenceUnits
     *            the persistence units
     */
    public KunderaJpaQuery(String... persistenceUnits)
    {
        this.persistenceUnits = persistenceUnits;
    }

    /**
     * Sets the grouping.
     * 
     * @param groupingClause
     *            the new grouping
     */
    @Override
    public void setGrouping(String groupingClause)
    {
    }

    /**
     * Sets the result.
     * 
     * @param result
     *            the new result
     */
    @Override
    public final void setResult(String result)
    {
        this.result = result;
    }

    /**
     * Sets the from.
     * 
     * @param from
     *            the new from
     */
    @Override
    public final void setFrom(String from)
    {
        this.from = from;
    }

    /**
     * Sets the filter.
     * 
     * @param filter
     *            the new filter
     */
    @Override
    public final void setFilter(String filter)
    {
        this.filter = filter;
    }

    /**
     * Sets the ordering.
     * 
     * @param ordering
     *            the new ordering
     */
    @Override
    public final void setOrdering(String ordering)
    {
        this.ordering = ordering;
        parseOrdering(ordering);
    }

    /**
     * Gets the filter.
     * 
     * @return the filter
     */
    @Override
    public final String getFilter()
    {
        return filter;
    }

    /**
     * Gets the from.
     * 
     * @return the from
     */
    @Override
    public final String getFrom()
    {
        return from;
    }

    /**
     * Gets the ordering.
     * 
     * @return the ordering
     */
    @Override
    public final List<SortOrdering> getOrdering()
    {
        return sortOrders;
    }

    /**
     * Gets the result.
     * 
     * @return the result
     */
    @Override
    public final String getResult()
    {
        return result;
    }

    /**
     * Method to check if required result is to get complete entity or a select
     * scalar value.
     * 
     * @return true, if it result is for complete alias.
     * 
     */
    @Override
    public final boolean isAliasOnly()
    {
        return result != null && (result.indexOf(".") == -1);
    }

    // must be executed after parse(). it verifies and populated the query
    // predicates.
    /**
     * Post parsing init.
     */
    @Override
    public void postParsingInit()
    {
        initEntityClass();
        initFilter();
    }

    /**
     * Inits the entity class.
     */
    private void initEntityClass()
    {
        // String result = getResult();
        // String from = getFrom();

        String fromArray[] = from.split(" ");
        if (fromArray.length != 2)
        {
            throw new PersistenceException("Bad query format: " + from);
        }

        StringTokenizer tokenizer = new StringTokenizer(getResult(), ",");
        while (tokenizer.hasMoreTokens())
        {
            String token = tokenizer.nextToken();
            if (!StringUtils.containsAny(fromArray[1] + ".", token))
            {
                throw new RuntimeException("bad query format with invalid alias:" + token);
            }
        }
        /*
         * if (!fromArray[1].equals(result)) { throw new
         * PersistenceException("Bad query format: " + from); }
         */
        this.entityName = fromArray[0];
        this.entityAlias = fromArray[1];

        entityClass = getMetamodel().getEntityClass(entityName);

        if (null == entityClass)
        {
            throw new PersistenceException("No entity found by the name: " + entityName);
        }
        EntityMetadata metadata = getMetamodel().getEntityMetadata(entityClass);
        if (!metadata.isIndexable())
        {
            throw new PersistenceException(entityClass + " is not indexed. What are you searching for dude?");
        }
    }

    /**
     * Inits the filter.
     */
    private void initFilter()
    {
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(entityClass, persistenceUnits);
        String indexName = metadata.getIndexName();

        // String filter = getFilter();

        if (null == filter)
        {
            return;
        }

        List<String> clauses = tokenize(filter, INTER_CLAUSE_PATTERN);

        // parse and structure for "between" clause , if present, else it will
        // return original clause
        clauses = parseFilterForBetweenClause(clauses, indexName);
        // clauses must be alternate Inter and Intra combination, starting with
        // Intra.
        boolean newClause = true;
        for (String clause : clauses)
        {

            if (newClause)
            {
                List<String> tokens = tokenize(clause, INTRA_CLAUSE_PATTERN);

                if (tokens.size() != 3)
                {
                    throw new PersistenceException("bad jpa query: " + clause);
                }

                // strip alias from property name
                String property = tokens.get(0);
                property = property.substring((entityAlias + ".").length());
                property = indexName + "." + property;
                // verify condition
                String condition = tokens.get(1);
                if (!Arrays.asList(INTRA_CLAUSE_OPERATORS).contains(condition.toUpperCase()))
                {
                    throw new PersistenceException("bad jpa query: " + clause);
                }

                filtersQueue.add(new FilterClause(property, condition, tokens.get(2)));
                newClause = false;
            }

            else
            {
                if (Arrays.asList(INTER_CLAUSE_OPERATORS).contains(clause.toUpperCase()))
                {
                    filtersQueue.add(clause.toUpperCase());
                    newClause = true;
                }
                else
                {
                    throw new PersistenceException("bad jpa query: " + clause);
                }
            }
        }
    }

    /**
     * Sets the parameter.
     * 
     * @param name
     *            the name
     * @param value
     *            the value
     */
    @Override
    public final void setParameter(String name, String value)
    {
        boolean found = false;
        for (Object object : getFilterClauseQueue())
        {
            if (object instanceof FilterClause)
            {
                FilterClause filter = (FilterClause) object;
                // key
                if (filter.getValue().equals(":" + name))
                {
                    filter.setValue(value);
                    found = true;
                    return;
                }
            }
        }
        if (!found)
        {
            throw new PersistenceException("invalid parameter: " + name);
        }
    }

    /**
     * Gets the entity class.
     * 
     * @return the entityClass
     */
    @Override
    public final Class getEntityClass()
    {
        return entityClass;
    }

    /**
     * Gets the entity metadata.
     * 
     * @return the entity metadata
     */
    @Override
    public final EntityMetadata getEntityMetadata()
    {
        return KunderaMetadataManager.getEntityMetadata(entityClass, persistenceUnits);
    }

    /**
     * Gets the filter clause queue.
     * 
     * @return the filters
     */
    @Override
    public final Queue getFilterClauseQueue()
    {
        return filtersQueue;
    }

    // class to keep hold of a where clause predicate
    /**
     * The Class FilterClause.
     */
    public final class FilterClause
    {

        /** The property. */
        private String property;

        /** The condition. */
        private String condition;

        /** The value. */
        String value;

        /**
         * The Constructor.
         * 
         * @param property
         *            the property
         * @param condition
         *            the condition
         * @param value
         *            the value
         */
        public FilterClause(String property, String condition, String value)
        {
            super();
            this.property = property;
            this.condition = condition;
            this.value = value;
        }

        /**
         * Gets the property.
         * 
         * @return the property
         */
        public final String getProperty()
        {
            return property;
        }

        /**
         * Gets the condition.
         * 
         * @return the condition
         */
        public final String getCondition()
        {
            return condition;
        }

        /**
         * Gets the value.
         * 
         * @return the value
         */
        public final String getValue()
        {
            return value;
        }

        /**
         * Sets the value.
         * 
         * @param value
         *            the value to set
         */
        protected void setValue(String value)
        {
            this.value = value;
        }

        /* @see java.lang.Object#toString() */
        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            builder.append("FilterClause [property=");
            builder.append(property);
            builder.append(", condition=");
            builder.append(condition);
            builder.append(", value=");
            builder.append(value);
            builder.append("]");
            return builder.toString();
        }
    }

    /* @see java.lang.Object#clone() */
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#clone()
     */
    @Override
    public final Object clone() throws CloneNotSupportedException
    {
        return super.clone();
    }

    /* @see java.lang.Object#toString() */
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public final String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("KunderaQuery [entityName=");
        builder.append(entityName);
        builder.append(", entityAlias=");
        builder.append(entityAlias);
        builder.append(", filtersQueue=");
        builder.append(filtersQueue);
        builder.append("]");
        return builder.toString();
    }

    // helper method
    /**
     * Tokenize.
     * 
     * @param where
     *            the where
     * @param pattern
     *            the pattern
     * @return the list
     */
    private static List<String> tokenize(String where, Pattern pattern)
    {
        List<String> split = new ArrayList<String>();
        Matcher matcher = pattern.matcher(where);
        int lastIndex = 0;
        String s;
        // int count = 0;
        while (matcher.find())
        {
            s = where.substring(lastIndex, matcher.start()).trim();
            split.add(s);
            s = matcher.group();
            split.add(s.toUpperCase());
            lastIndex = matcher.end();
            // count++;
        }
        s = where.substring(lastIndex).trim();
        split.add(s);
        return split;
    }

    /**
     * Gets the metamodel.
     * 
     * @return the metamodel
     */
    private MetamodelImpl getMetamodel()
    {
        return KunderaMetadataManager.getMetamodel(persistenceUnits);
    }

    /**
     * Gets the persistence units.
     * 
     * @return the persistenceUnits
     */
    @Override
    public String[] getPersistenceUnits()
    {
        return persistenceUnits;
    }

    /**
     * Sets the persistence units.
     * 
     * @param persistenceUnits
     *            the persistenceUnits to set
     */
    @Override
    public void setPersistenceUnits(String[] persistenceUnits)
    {
        this.persistenceUnits = persistenceUnits;
    }

    /**
     * Parses the ordering @See Order By Clause.
     * 
     * @param ordering
     *            the ordering
     */
    private void parseOrdering(String ordering)
    {
        final String comma = ",";
        final String space = " ";

        StringTokenizer tokenizer = new StringTokenizer(ordering, comma);

        sortOrders = new ArrayList<KunderaJpaQuery.SortOrdering>();
        while (tokenizer.hasMoreTokens())

        {
            String order = (String) tokenizer.nextElement();
            StringTokenizer token = new StringTokenizer(order, space);
            SortOrder orderType = SortOrder.ASC;

            String colName = (String) token.nextElement();
            while (token.hasMoreElements())
            {

                String nextOrder = (String) token.nextElement();

                // more spaces given.
                if (StringUtils.isNotBlank(nextOrder))
                {
                    try
                    {
                        orderType = SortOrder.valueOf(nextOrder.toUpperCase());
                    }
                    catch (IllegalArgumentException e)
                    {
                        logger.error("Error while parsing order by clause:");
                        throw new KunderaQueryParserException("Invalid sort order provided:" + nextOrder);
                    }
                }
            }

            sortOrders.add(new SortOrdering(colName, orderType));
        }
    }

    /**
     * Containing SortOrder.
     */
    public class SortOrdering
    {

        /** The column name. */
        String columnName;

        /** The order. */
        SortOrder order;

        /**
         * Instantiates a new sort ordering.
         * 
         * @param columnName
         *            the column name
         * @param order
         *            the order
         */
        public SortOrdering(String columnName, SortOrder order)
        {
            this.columnName = columnName;
            this.order = order;
        }

        /**
         * Gets the column name.
         * 
         * @return the column name
         */
        public String getColumnName()
        {
            return columnName;
        }

        /**
         * Gets the order.
         * 
         * @return the order
         */
        public SortOrder getOrder()
        {
            return order;
        }
    }

    /**
     * The Enum SortOrder.
     */
    public enum SortOrder
    {

        /** The ASC. */
        ASC,
        /** The DESC. */
        DESC;
    }

    /**
     * Return parsed token string.
     * 
     * @param tokens
     *            inter claues token string.
     * @param indexName
     *            table name
     * @return tokens converted to "<=" and ">=" clause
     */
    private List<String> parseFilterForBetweenClause(List<String> tokens, String indexName)
    {
        final String between = "BETWEEN";

        if (tokens.contains(between))
        {
            // change token set to parse and compile.
            int idxOfBetween = tokens.indexOf(between);
            String property = tokens.get(idxOfBetween - 1);
            // property = property.substring((entityAlias + ".").length());
            // property = indexName + "." + property;
            Matcher match = INTRA_CLAUSE_PATTERN.matcher(property);
            // in case any intra clause given along with column name.
            if (match.find())
            {
                logger.error("bad jpa query:");
                throw new KunderaQueryParserException("invalid column name " + property);
            }
            String minValue = tokens.get(idxOfBetween + 1);
            String maxValue = tokens.get(idxOfBetween + 3);

            tokens.set(idxOfBetween + 1, property + ">=" + minValue);
            tokens.set(idxOfBetween + 3, property + "<=" + maxValue);
            tokens.remove(idxOfBetween - 1);
            tokens.remove(idxOfBetween - 1);
        }

        return tokens;
    }
}
