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
package com.impetus.client.cassandra.pelops.property;

import com.impetus.client.cassandra.pelops.composite.Composite;
import com.impetus.client.cassandra.pelops.composite.CompositeAccessor;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.PersistenceException;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.proxy.EnhancedEntity;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * Helper class to access fields.
 * 
 * @author animesh.kumar
 */
public class CassPropertyAccessorHelper
{

    /**
     * Sets an object onto a field.
     * 
     * @param target
     *            the target
     * @param field
     *            the field
     * @param value
     *            the value
     * 
     * @throws PropertyAccessException
     *             the property access exception
     */
    public static void set(Object target, Field field, Object value) throws PropertyAccessException
    {

        if (!field.isAccessible())
        {
            field.setAccessible(true);
        }
        try
        {
            field.set(target, value);
        }
        catch (IllegalArgumentException iarg)
        {
            throw new PropertyAccessException(iarg);
        }
        catch (IllegalAccessException iacc)
        {
            throw new PropertyAccessException(iacc);
        }
    }

    /**
     * Gets object from field.
     * 
     * @param from
     *            the from
     * @param field
     *            the field
     * 
     * @return the object
     * 
     * @throws PropertyAccessException
     *             the property access exception
     */
    public static Object getObject(Object from, Field field) throws PropertyAccessException
    {

        if (!field.isAccessible())
        {
            field.setAccessible(true);
        }

        try
        {
            return field.get(from);
        }
        catch (IllegalArgumentException iarg)
        {
            throw new PropertyAccessException(iarg);
        }
        catch (IllegalAccessException iacc)
        {
            throw new PropertyAccessException(iacc);
        }
    }

    /**
     * Gets the string.
     * 
     * @param from
     *            the from
     * @param field
     *            the field
     * 
     * @return the string
     * 
     * @throws PropertyAccessException
     *             the property access exception
     */
    public static String getString(Object from, Field field) throws PropertyAccessException
    {

        PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor(field);
        Object object = getObject(from, field);
        return object != null ? accessor.toString(object) : null;

    }

    /**
     * Gets field value as byte-array.
     * 
     * @param from
     *            the from
     * @param field
     *            the field
     * 
     * @return the byte[]
     * 
     * @throws PropertyAccessException
     *             the property access exception
     */
    public static byte[] get(Object from, Field field) throws PropertyAccessException
    {
        PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor(field);
        return accessor.toBytes(getObject(from, field));
    }

    /**
     * Get identifier of an entity object by invoking getXXX() method.
     * 
     * 
     * @param entity
     *            the entity
     * @param metadata
     *            the metadata
     * 
     * @return the id
     * 
     * @throws PropertyAccessException
     *             the property access exception
     */
    public static String getId(Object entity, EntityMetadata metadata) throws PropertyAccessException
    {

        // If an Entity has been wrapped in a Proxy, we can call the Proxy
        // classes' getId() method
        if (entity instanceof EnhancedEntity)
        {

            return ((EnhancedEntity) entity).getId();
        }

        // Otherwise, as Kundera currently supports only field access, access
        // the underlying Entity's id field
        return getString(entity, metadata.getIdColumn().getField());
    }

    /**
     * Sets Primary Key (Row key) into entity field that was annotated with @Id.
     *
     * @param entity the entity
     * @param metadata the metadata
     * @param rowKey the row key
     * @throws PropertyAccessException the property access exception
     */
    public static void setId(Object entity, EntityMetadata metadata, String rowKey) throws PropertyAccessException
    {
        try
        {
            Object obj;
            Field field = metadata.getIdColumn().getField();
            if (field.getType().equals(Composite.class))
            {
                CompositeAccessor accessor = new CompositeAccessor();
                obj = accessor.fromString(rowKey, field);
            }
            else
            {
                PropertyAccessor<?> accessor = PropertyAccessorFactory.getPropertyAccessor(metadata.getIdColumn()
                        .getField());
                obj = accessor.fromString(rowKey);
            }
            metadata.getWriteIdentifierMethod().invoke(entity, obj);
        }
        catch (IllegalArgumentException iarg)
        {
            throw new PropertyAccessException(iarg);
        }
        catch (IllegalAccessException iacc)
        {
            throw new PropertyAccessException(iacc);
        }
        catch (InvocationTargetException ite)
        {
            throw new PropertyAccessException(ite);
        }
    }

    /**
     * Gets the embedded object.
     * 
     * @param obj
     *            the obj
     * @param fieldName
     *            the field name
     * @return the embedded object
     * @throws PropertyAccessException
     *             the property access exception
     */
    @SuppressWarnings("null")
    // TODO: Too much code, improve this, possibly by breaking it
    public static final Object getObject(Object obj, String fieldName) throws PropertyAccessException
    {
        Field embeddedField;
        try
        {
            embeddedField = obj.getClass().getDeclaredField(fieldName);
            if (embeddedField != null)
            {
                if (!embeddedField.isAccessible())
                {
                    embeddedField.setAccessible(true);
                }
                Object embededObject = embeddedField.get(obj);
                if (embededObject == null)
                {
                    Class embeddedObjectClass = embeddedField.getType();
                    if (Collection.class.isAssignableFrom(embeddedObjectClass))
                    {
                        if (embeddedObjectClass.equals(List.class))
                        {
                            return new ArrayList();
                        }
                        else if (embeddedObjectClass.equals(Set.class))
                        {
                            return new HashSet();
                        }
                    }
                    else
                    {
                        embededObject = embeddedField.getType().newInstance();
                        embeddedField.set(obj, embededObject);
                    }

                }
                return embededObject;
            }
            else
            {
                throw new RuntimeException("Embedded object not found: " + fieldName);
            }

        }
        catch (SecurityException e)
        {
            throw new PropertyAccessException(e);
        }
        catch (NoSuchFieldException e)
        {
            throw new PropertyAccessException(e);
        }
        catch (IllegalArgumentException e)
        {
            throw new PropertyAccessException(e);
        }
        catch (IllegalAccessException e)
        {
            throw new PropertyAccessException(e);
        }
        catch (InstantiationException e)
        {
            throw new PropertyAccessException(e);
        }
    }

    /**
     * Retrieves Generic class from a collection field.
     * 
     * @param collectionField
     *            the collection field
     * @return the generic class
     */
    public static Class<?> getGenericClass(Field collectionField)
    {
        Class<?> genericClass = null;
        if (collectionField == null)
        {
            return genericClass;
        }
        if (isCollection(collectionField.getType()))
        {

            Type[] parameters = ReflectUtils.getTypeArguments(collectionField);
            if (parameters != null)
            {
                if (parameters.length == 1)
                {
                    genericClass = (Class<?>) parameters[0];
                }
                else
                {
                    throw new PersistenceException(
                            "Can't determine generic class from a field that has two parameters.");
                }
            }
        }
        return genericClass != null ? genericClass : collectionField.getType();
    }

    /**
     * Gets the declared fields.
     * 
     * @param relationalField
     *            the relational field
     * @return the declared fields
     */
    public static Field[] getDeclaredFields(Field relationalField)
    {
        Field[] fields;
        if (isCollection(relationalField.getType()))
        {
            fields = CassPropertyAccessorHelper.getGenericClass(relationalField).getDeclaredFields();
        }
        else
        {
            fields = relationalField.getType().getDeclaredFields();
        }
        return fields;
    }

    /**
     * Checks if is collection.
     *
     * @param clazz the clazz
     * @return true, if is collection
     */
    public static final boolean isCollection(Class<?> clazz)
    {
        return Collection.class.isAssignableFrom(clazz) /*
                                                         * ||
                                                         * clazz.isAssignableFrom
                                                         * (Set.class)
                                                         */;

    }

}
