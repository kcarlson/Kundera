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
package com.impetus.kundera.metadata.model;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.persistence.SharedCacheMode;
import javax.persistence.ValidationMode;
import javax.persistence.spi.ClassTransformer;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.PersistenceUnitTransactionType;
import javax.sql.DataSource;

/**
 * @author amresh.singh
 * 
 */
public class PersistenceUnitMetadata implements PersistenceUnitInfo
{
    /** Persistence Unit name */
    private String persistenceUnitName;

    /** The provider. */
    private String provider;

    /** The transaction type. */
    private PersistenceUnitTransactionType transactionType;

    /** The classes. */
    private List<String> classes = new ArrayList<String>();

    /** The packages. */
    private List<String> packages = new ArrayList<String>();

    /** The jar files. */
    private Set<String> jarFiles = new HashSet<String>();

    /** The properties. */
    private Properties properties = new Properties();

    /** The exclude unlisted classes. */
    private boolean excludeUnlistedClasses = false;

    @Override
    public String getPersistenceUnitName()
    {
        return persistenceUnitName;
    }

    /**
     * @param persistenceUnitName
     *            the persistenceUnitName to set
     */
    public void setPersistenceUnitName(String persistenceUnitName)
    {
        this.persistenceUnitName = persistenceUnitName;
    }

    /**
     * Sets the transaction type.
     * 
     * @param transactionType
     *            the new transaction type
     */
    public void setTransactionType(PersistenceUnitTransactionType transactionType)
    {
        this.transactionType = transactionType;
    }

    /**
     * Gets the provider.
     * 
     * @return the provider
     */
    public String getProvider()
    {
        return provider;
    }

    /**
     * Sets the provider.
     * 
     * @param provider
     *            the new provider
     */
    public void setProvider(String provider)
    {
        if (provider != null && provider.endsWith(".class"))
        {
            this.provider = provider.substring(0, provider.length() - 6);
        }
        this.provider = provider;
    }

    /**
     * Gets the classes.
     * 
     * @return the classes
     */
    public List<String> getClasses()
    {
        return classes;
    }

    /**
     * Sets the classes.
     * 
     * @param classes
     *            the new classes
     */
    public void setClasses(List<String> classes)
    {
        this.classes = classes;
    }

    /**
     * Gets the packages.
     * 
     * @return the packages
     */
    public List<String> getPackages()
    {
        return packages;
    }

    /**
     * Sets the packages.
     * 
     * @param packages
     *            the new packages
     */
    public void setPackages(List<String> packages)
    {
        this.packages = packages;
    }

    /**
     * Gets the jar files.
     * 
     * @return the jar files
     */
    public Set<String> getJarFiles()
    {
        return jarFiles;
    }

    /**
     * Sets the jar files.
     * 
     * @param jarFiles
     *            the new jar files
     */
    public void setJarFiles(Set<String> jarFiles)
    {
        this.jarFiles = jarFiles;
    }

    /**
     * Gets the exclude unlisted classes.
     * 
     * @return the exclude unlisted classes
     */
    public boolean getExcludeUnlistedClasses()
    {
        return excludeUnlistedClasses;
    }

    /**
     * Sets the exclude unlisted classes.
     * 
     * @param excludeUnlistedClasses
     *            the new exclude unlisted classes
     */
    public void setExcludeUnlistedClasses(boolean excludeUnlistedClasses)
    {
        this.excludeUnlistedClasses = excludeUnlistedClasses;
    }

    /* @see java.lang.Object#toString() */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("PersistenceMetadata [name=");
        builder.append(persistenceUnitName);
        builder.append(", provider=");
        builder.append(provider);
        builder.append(", transactionType=");
        builder.append(transactionType);
        builder.append(", classes=");
        builder.append(classes);
        builder.append(", excludeUnlistedClasses=");
        builder.append(excludeUnlistedClasses);
        builder.append(", jarFiles=");
        builder.append(jarFiles);
        builder.append(", packages=");
        builder.append(packages);
        builder.append(", props=");
        builder.append(properties);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public String getPersistenceProviderClassName()
    {
        return null;
    }

    @Override
    public PersistenceUnitTransactionType getTransactionType()
    {
        return null;
    }

    @Override
    public DataSource getJtaDataSource()
    {
        return null;
    }

    @Override
    public DataSource getNonJtaDataSource()
    {
        return null;
    }

    @Override
    public List<String> getMappingFileNames()
    {
        return null;
    }

    @Override
    public List<URL> getJarFileUrls()
    {
        return null;
    }

    @Override
    public URL getPersistenceUnitRootUrl()
    {
        return null;
    }

    @Override
    public List<String> getManagedClassNames()
    {
        return null;
    }

    @Override
    public boolean excludeUnlistedClasses()
    {
        return false;
    }

    @Override
    public SharedCacheMode getSharedCacheMode()
    {
        return null;
    }

    @Override
    public ValidationMode getValidationMode()
    {
        return null;
    }

    @Override
    public Properties getProperties()
    {
        return this.properties;
    }

    /**
     * @param properties
     *            the properties to set
     */
    public void setProperties(Properties properties)
    {
        this.properties = properties;
    }

    @Override
    public String getPersistenceXMLSchemaVersion()
    {
        return null;
    }

    @Override
    public ClassLoader getClassLoader()
    {
        return null;
    }

    @Override
    public void addTransformer(ClassTransformer paramClassTransformer)
    {
    }

    @Override
    public ClassLoader getNewTempClassLoader()
    {
        return null;
    }

}
