package com.impetus.kundera.metadata.model;

import java.lang.reflect.Field;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.FetchType;

/**
 * The Class Relation.
 */
public final class Relation
{

    /** The property. */
    private Field property;

    /** The target entity. */
    private Class<?> targetEntity;

    /** The property type. */
    private Class<?> propertyType;

    /** The fetch type. */
    FetchType fetchType;

    /** The cascades. */
    private List<CascadeType> cascades;

    /** The optional. */
    private boolean optional;

    /** The mapped by. */
    private String mappedBy;

    /** The type. */
    private Relation.ForeignKey type;

    /**
     * 
     * The Enum ForeignKey.
     */
    public static enum ForeignKey
    {
        /** The ON e_ t o_ one. */
        ONE_TO_ONE,
        /** The ON e_ t o_ many. */
        ONE_TO_MANY,
        /** The MAN y_ t o_ one. */
        MANY_TO_ONE,
        /** The MAN y_ t o_ many. */
        MANY_TO_MANY
    }

    /**
     * Instantiates a new relation.
     * 
     * @param property
     *            the property
     * @param targetEntity
     *            the target entity
     * @param propertyType
     *            the property type
     * @param fetchType
     *            the fetch type
     * @param cascades
     *            the cascades
     * @param optional
     *            the optional
     * @param mappedBy
     *            the mapped by
     * @param type
     *            the type
     */

    /**
     * Specifies the type of the metadata.
     */

    public Relation(Field property, Class<?> targetEntity, Class<?> propertyType, FetchType fetchType,
            List<CascadeType> cascades, boolean optional, String mappedBy, Relation.ForeignKey type)
    {
        super();
        this.property = property;
        this.targetEntity = targetEntity;
        this.propertyType = propertyType;
        this.fetchType = fetchType;
        this.cascades = cascades;
        this.optional = optional;
        this.mappedBy = mappedBy;
        this.type = type;
    }

    /**
     * Gets the property.
     * 
     * @return the property
     */
    public Field getProperty()
    {
        return property;
    }

    /**
     * Gets the target entity.
     * 
     * @return the targetEntity
     */
    public Class<?> getTargetEntity()
    {
        return targetEntity;
    }

    /**
     * Gets the property type.
     * 
     * @return the propertyType
     */
    public Class<?> getPropertyType()
    {
        return propertyType;
    }

    /**
     * Gets the fetch type.
     * 
     * @return the fetchType
     */
    public FetchType getFetchType()
    {
        return fetchType;
    }

    /**
     * Gets the cascades.
     * 
     * @return the cascades
     */
    public List<CascadeType> getCascades()
    {
        return cascades;
    }

    /**
     * Checks if is optional.
     * 
     * @return the optional
     */
    public boolean isOptional()
    {
        return optional;
    }

    /**
     * Gets the mapped by.
     * 
     * @return the mappedBy
     */
    public String getMappedBy()
    {
        return mappedBy;
    }

    /**
     * Gets the type.
     * 
     * @return the type
     */
    public Relation.ForeignKey getType()
    {
        return type;
    }

    /**
     * Checks if is unary.
     * 
     * @return true, if is unary
     */
    public boolean isUnary()
    {
        return type.equals(Relation.ForeignKey.ONE_TO_ONE) || type.equals(Relation.ForeignKey.MANY_TO_ONE);
    }

    /**
     * Checks if is collection.
     * 
     * @return true, if is collection
     */
    public boolean isCollection()
    {
        return type.equals(Relation.ForeignKey.ONE_TO_MANY) || type.equals(Relation.ForeignKey.MANY_TO_MANY);
    }
}