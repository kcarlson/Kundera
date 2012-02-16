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
package com.impetus.client.entity;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Embeddable;

import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * The Class PostData.
 */
@Embeddable
public class PostData
{
    /** The title. */
    @Column(name = "title")
    public String title;

    /** The body. */
    @Column(name = "body")
    public String body;

    /** The created. */
    @Column(name = "created")
    public Date created;

    /**
     * Instantiates a new post data.
     */
    public PostData()
    {
    }

    /**
     * Gets the title.
     *
     * @return the title
     */
    public String getTitle()
    {
        return title;
    }

    /**
     * Sets the title.
     *
     * @param title the title to set
     */
    public void setTitle(String title)
    {
        this.title = title;
    }

    /**
     * Gets the body.
     *
     * @return the body
     */
    public String getBody()
    {
        return body;
    }

    /**
     * Sets the body.
     *
     * @param body the body to set
     */
    public void setBody(String body)
    {
        this.body = body;
    }

    /**
     * Gets the created.
     *
     * @return the created
     */
    public Date getCreated()
    {
        return created;
    }

    /**
     * Sets the created.
     *
     * @param created the created to set
     */
    public void setCreated(Date created)
    {
        this.created = created;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("Post [title=");
        builder.append(title);
        builder.append(", body=");
        builder.append(body);
        builder.append(", created=");
        builder.append(created);
        builder.append("]");
        return builder.toString();
    }

}
