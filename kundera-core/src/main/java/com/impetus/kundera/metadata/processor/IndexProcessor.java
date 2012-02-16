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
package com.impetus.kundera.metadata.processor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Id;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.annotations.Index;
import com.impetus.kundera.metadata.MetadataProcessor;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.PropertyIndex;


/**
 * The Class BaseMetadataProcessor.
 * 
 * @author animesh.kumar
 */
public class IndexProcessor implements MetadataProcessor
{

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(IndexProcessor.class);

    /*
     * @see
     * com.impetus.kundera.metadata.MetadataProcessor#process(java.lang.Class,
     * com.impetus.kundera.metadata.EntityMetadata)
     */
    /* (non-Javadoc)
     * @see com.impetus.kundera.metadata.MetadataProcessor#process(java.lang.Class, com.impetus.kundera.metadata.model.EntityMetadata)
     */
    public final void process(final Class<?> clazz, EntityMetadata metadata)
    {
        metadata.setIndexName(clazz.getSimpleName());
        Index idx = clazz.getAnnotation(Index.class);
        List<String> columnsToBeIndexed = new ArrayList<String>();

        if (null != idx)
        {
            boolean isIndexable = idx.index();
            metadata.setIndexable(isIndexable);

            String indexName = idx.name();
            if (indexName != null && !indexName.isEmpty())
            {
                metadata.setIndexName(indexName);
            }
            else
            {
                metadata.setIndexName(clazz.getSimpleName());
            }

            if (idx.columns() != null && idx.columns().length != 0)
            {
                columnsToBeIndexed = Arrays.asList(idx.columns());
            }

            if (!isIndexable)
            {
                log.debug("@Entity " + clazz.getName() + " will not be indexed for "
                        + (columnsToBeIndexed.isEmpty() ? "all columns" : columnsToBeIndexed));
                return;
            }
        }

        log.debug("Processing @Entity " + clazz.getName() + " for Indexes.");

        // scan for fields
        for (Field f : clazz.getDeclaredFields())
        {
            if (f.isAnnotationPresent(Id.class))
            {
                String alias = f.getName();
                alias = getIndexName(f, alias);
                metadata.addIndexProperty(new PropertyIndex(f, alias));
            }
            else if (f.isAnnotationPresent(Column.class))
            {
                String alias = f.getName();
                alias = getIndexName(f, alias);

                if (columnsToBeIndexed.isEmpty() || columnsToBeIndexed.contains(alias))
                {
                    metadata.addIndexProperty(new PropertyIndex(f, alias));
                }

            }
        }
    }

    /**
     * Gets the index name.
     *
     * @param f the f
     * @param alias the alias
     * @return the index name
     */
    private String getIndexName(Field f, String alias)
    {
        if (f.isAnnotationPresent(Column.class))
        {
            Column c = f.getAnnotation(Column.class);
            alias = c.name().trim();
            if (alias.isEmpty())
            {
                alias = f.getName();
            }
        }
        return alias;
    }
}
