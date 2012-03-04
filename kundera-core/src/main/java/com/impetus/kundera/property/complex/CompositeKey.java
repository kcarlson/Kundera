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
package com.impetus.kundera.property.complex;

import com.google.common.primitives.Bytes;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author kcarlson
 */
public class CompositeKey<S,T>
{
    public static final char SEPARATOR = (char) 0xFF;

    private static final int BUFF = 8092;
    
    private final S partA;
    private final T partB;

    

    public CompositeKey(S partA, T partB)
    {
        this.partA = partA;
        this.partB = partB;
    }
    
    public S getPartA()
    {
        return partA;
    }
    
    public T getPartB()
    {
        return partB;
    }

    public byte[] toByteArray() throws PropertyAccessException
    {
        ByteBuffer bb = ByteBuffer.allocate(BUFF);
        
        PropertyAccessor propertyAccessor = PropertyAccessorFactory.getPropertyAccessor(partA.getClass());
        byte[] bytes = propertyAccessor.toBytes(partA);
        bb.put(bytes).putChar(SEPARATOR);
        
        propertyAccessor = PropertyAccessorFactory.getPropertyAccessor(partB.getClass());
        bytes = propertyAccessor.toBytes(partB);
        bb.put(bytes).putChar(SEPARATOR);

        return bb.array();
    }

    public static CompositeKey fromString(String s, EntityMetadata metadata)
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public static CompositeKey fromBytes(byte[] bytes, EntityMetadata metadata)
    {
        CompositeKey result = null;
        try
        {
            Column idColumn = metadata.getIdColumn();
            Field field = idColumn.getField();
            CompositeKey ck = null;
            Object inst = metadata.getEntityClazz().newInstance();
            
            ck = (CompositeKey)field.get(inst);
            int idx = Bytes.indexOf(bytes, (byte) SEPARATOR);

            ByteBuffer bb1 = ByteBuffer.wrap(bytes, 0, idx);
            ByteBuffer bb2 = ByteBuffer.wrap(bytes, idx+1, bytes.length);
            
            PropertyAccessor propertyAccessor = PropertyAccessorFactory.getPropertyAccessor(ck.getPartA().getClass());
            
            Object obj1 = propertyAccessor.fromBytes(bb1.array());
            
            propertyAccessor = PropertyAccessorFactory.getPropertyAccessor(ck.getPartB().getClass());
            
            Object obj2 = propertyAccessor.fromBytes(bb2.array());
            
            
            result = new CompositeKey(obj1, obj2);
        }
        catch (InstantiationException ex)
        {
            Logger.getLogger(CompositeKey.class.getName()).log(Level.SEVERE, null, ex);
        }
        catch (IllegalArgumentException ex)
        {
            Logger.getLogger(CompositeKey.class.getName()).log(Level.SEVERE, null, ex);
        }
        catch (IllegalAccessException ex)
        {
            Logger.getLogger(CompositeKey.class.getName()).log(Level.SEVERE, null, ex);
        }
        catch (PropertyAccessException ex)
        {
            Logger.getLogger(CompositeKey.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return result;
    }

    @Override
    public String toString()
    {
        return super.toString();
    }
    
    
}
