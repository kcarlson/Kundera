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
package com.impetus.client.cassandra.pelops;

import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SuperColumn;

/**
 * Utility class that represents a row in Cassandra DB.
 *
 * @author animesh.kumar
 */
public class ThriftRow
{

    /**
     * name of the family.
     */
    private String columnFamilyName;

    /**
     * list of thrift columns from the row.
     */
    private List<Column> columns;

    /**
     * Id of the row.
     */
    private String id;

    /**
     * list of thrift super columns columns from the row.
     */
    private List<SuperColumn> superColumns;

    /**
     * default constructor.
     */
    public ThriftRow()
    {
        columns = new ArrayList<Column>();
        superColumns = new ArrayList<SuperColumn>();
    }

    /**
     * The Constructor.
     *
     * @param id the id
     * @param columnFamilyName the column family name
     * @param columns the columns
     * @param superColumns the super columns
     */
    public ThriftRow(String id, String columnFamilyName, List<Column> columns, List<SuperColumn> superColumns)
    {
        this.id = id;
        this.columnFamilyName = columnFamilyName;

        if (columns != null)
        {
            this.columns = columns;
        }

        if (superColumns != null)
        {
            this.superColumns = superColumns;
        }
    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    public String getId()
    {
        return id;
    }

    /**
     * Sets the id.
     *
     * @param id the key to set
     */
    public void setId(String id)
    {
        this.id = id;
    }

    /**
     * Gets the column family name.
     *
     * @return the columnFamilyName
     */
    public String getColumnFamilyName()
    {
        return columnFamilyName;
    }

    /**
     * Sets the column family name.
     *
     * @param columnFamilyName the columnFamilyName to set
     */
    public void setColumnFamilyName(String columnFamilyName)
    {
        this.columnFamilyName = columnFamilyName;
    }

    /**
     * Gets the columns.
     *
     * @return the columns
     */
    public List<Column> getColumns()
    {
        return columns;
    }

    /**
     * Sets the columns.
     *
     * @param columns the columns to set
     */
    public void setColumns(List<Column> columns)
    {
        this.columns = columns;
    }

    /**
     * Adds the column.
     *
     * @param column the column
     */
    public void addColumn(Column column)
    {
        columns.add(column);
    }

    /**
     * Gets the super columns.
     *
     * @return the superColumns
     */
    public List<SuperColumn> getSuperColumns()
    {
        return superColumns;
    }

    /**
     * Sets the super columns.
     *
     * @param superColumns the superColumns to set
     */
    public void setSuperColumns(List<SuperColumn> superColumns)
    {
        this.superColumns = superColumns;
    }

    /**
     * Adds the super column.
     *
     * @param superColumn the super column
     */
    public void addSuperColumn(SuperColumn superColumn)
    {
        this.superColumns.add(superColumn);
    }
}