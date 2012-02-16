/**
 * 
 */
package com.impetus.client.rdbms;

import java.util.Properties;

import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * The Class HibernateUtils.
 *
 * @author vivek.mishra
 */
public final class HibernateUtils
{

    /**
     * Gets the properties.
     *
     * @param persistenceUnit the persistence unit
     * @return the properties
     */
    static final Properties getProperties(final String persistenceUnit)
    {
        PersistenceUnitMetadata persistenceUnitMetadatata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit);
        Properties props = persistenceUnitMetadatata.getProperties();
        return props;
    }

}
