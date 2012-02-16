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
package com.impetus.kundera.index;

import java.util.Map;

import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * Interface to define the behavior of an Indexer.
 * 
 * @author animesh.kumar
 */
public interface Indexer
{

    /**
     * Unindexed an entity with key:id.
     * 
     * @param metadata
     *            the metadata
     * @param id
     *            the id
     */
    void unindex(EntityMetadata metadata, String id);

    /**
     * Indexes and object.
     * 
     * @param metadata
     *            the metadata
     * @param object
     *            the object
     */
    void index(EntityMetadata metadata, Object object);

    /**
     * Indexes and object.
     * 
     * @param metadata
     *            the meta data.
     * @param object
     *            the object.
     * @param parentId
     *            parent Id.
     * @param clazz
     *            parent class.
     */
    void index(EntityMetadata metadata, Object object, String parentId, Class<?> clazz);

    /**
     * Searches for an object. Note that the "query" must be in Indexer
     * specified form.
     *
     * @param luceneQuery the lucene query
     * @param start the start
     * @param count the count
     * @param fetchRelation the fetch relation
     * @return the list
     */

    Map<String, String> search(String luceneQuery, int start, int count, boolean fetchRelation);

    /**
     * Close on index writer/reader.
     */
    void close();

    /**
     * Flushes out indexes.
     */
    void flush();
}
