/*******************************************************************************
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
package com.impetus.client.cassandra.pelops.composite;

import com.impetus.kundera.property.PropertyAccessException;
import java.lang.reflect.Field;

/**
 *
 * @author kcarlson
 */
public class CompositeAccessor
{

    public Composite fromBytes(byte[] bytes, Field field) throws PropertyAccessException
    {
        try
        {
            return Composite.parse(bytes, field);
        }
        catch (Exception e)
        {
            throw new PropertyAccessException(e.getMessage());
        }
    }

    public byte[] toBytes(Object object) throws PropertyAccessException
    {
        try
        {
            Composite composite = (Composite) object;
            return composite.serialize();
        }
        catch (Exception e)
        {
            throw new PropertyAccessException(e.getMessage());
        }
    }

    public String toString(Object object)
    {
        return ((Composite) object).toString();
    }

    public Composite fromString(String s) throws PropertyAccessException
    {
        try
        {
            return Composite.fromString(s);
        }
        catch (Exception e)
        {
            throw new PropertyAccessException(e.getMessage());
        }
    }

}
