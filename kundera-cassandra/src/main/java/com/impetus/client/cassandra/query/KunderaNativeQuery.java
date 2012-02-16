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

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.query.KunderaJpaQuery.SortOrdering;
import com.impetus.kundera.query.KunderaQuery;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import javax.persistence.Parameter;

/**
 *
 * @author kcarlson
 */
public class KunderaNativeQuery implements KunderaQuery
{
    
    private String[] persistenceUnits;
    private String entityName;
    private EntityMetadata entityMetadata;
    private Class<?> entityClass;

    public KunderaNativeQuery(String[] persistenceUnits)
    {
        this.persistenceUnits = persistenceUnits;
    }
    
    @Override
    public void postParsingInit()
    {
        MetamodelImpl metamodel = KunderaMetadataManager.getMetamodel(persistenceUnits);
        
        Class clazz = metamodel.getEntityClass(entityName);
        entityMetadata = KunderaMetadataManager.getEntityMetadata(clazz, persistenceUnits);
        
        entityClass = getMetamodel().getEntityClass(entityName);
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
    
    @Override
    public void setGrouping(String groupingClause)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setResult(String result)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setFrom(String from)
    {
        this.entityName = from;
    }

    @Override
    public void setFilter(String filter)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setOrdering(String ordering)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getFilter()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getFrom()
    {
        return entityName;
    }

    @Override
    public List<SortOrdering> getOrdering()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getResult()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isAliasOnly()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setParameter(String name, String value)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Class getEntityClass()
    {
        return entityClass;
    }

    @Override
    public EntityMetadata getEntityMetadata()
    {
        return entityMetadata;
    }

    @Override
    public Queue getFilterClauseQueue()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String[] getPersistenceUnits()
    {
        return persistenceUnits;
    }

    @Override
    public void setPersistenceUnits(String[] persistenceUnits)
    {
        this.persistenceUnits = persistenceUnits;
    }

    @Override
    public Set<Parameter<?>> getParameters()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Parameter<?> getParameter(String paramString)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object getParameterValue(String paramString)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
