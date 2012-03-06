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
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.types.CompositeType;

public class Composite implements Comparable<Composite>, Serializable
{
    static final Logger logger = Logger.getLogger(Composite.class.getName());

    private List<Object> parts;

    private CompositeType.Builder builder;

    /**
     * 
     */
    public Composite()
    {
        builder = CompositeType.Builder.newBuilder();
        parts = new ArrayList<Object>();
    }

    @Override
    public String toString()
    {
        Iterator<Object> iter = parts.iterator();

        if (!iter.hasNext())
        {
            return "";
        }

        StringBuilder sb = new StringBuilder(String.valueOf(iter.next()));

        while (iter.hasNext())
        {
            sb.append(":").append(String.valueOf(iter.next()));
        }
        return sb.toString();//.substring(0, sb.length() - 1);
    }

    public static Composite fromObjects(Object... parts)
    {
        Composite composite = new Composite();
        for (Object o : parts)
        {
            if (o instanceof Long)
            {
                composite.addLong((Long) o);
            }
            else if (o instanceof Integer)
            {
                composite.addInt((Integer) o);
            }
            else if (o instanceof Double)
            {
                composite.addDouble((Double) o);
            }
            else if (o instanceof Float)
            {
                composite.addFloat((Float) o);
            }
            else if (o instanceof Boolean)
            {
                composite.addBoolean((Boolean) o);
            }
            else if (o instanceof String)
            {
                composite.addUTF8((String) o);
            }
            else if (o instanceof UUID)
            {
                composite.addUuid((UUID) o);
            }
            else if (o instanceof byte[])
            {
                composite.addByteArray((byte[]) o);
            }
        }

        return composite;
    }

    public static Composite fromString(String s, Field field)
    {
        Composite composite = new Composite();
        String[] compositeParts = s.split(":");
        Class[] compositeTypes = getCompositeFieldTypes(field);

        if (compositeTypes == null || compositeParts.length != compositeTypes.length)
        {
            throw new MarshalException("Supplied byte array did not parse correctly "
                    + "or composite field not annotated correctly.");
        }

        for (int i = 0; i < compositeParts.length; i++)
        {
            try
            {
                String part = compositeParts[i];
                Class cl = compositeTypes[i];
                PropertyAccessor accessor = PropertyAccessorFactory.getPropertyAccessor(cl);
                Object value = accessor.fromString(part);

                if (cl.equals(Long.class))
                {
                    composite.addLong((Long) value);
                }
                else if (cl.equals(Integer.class))
                {
                    composite.addInt((Integer) value);
                }
                else if (cl.equals(Double.class))
                {
                    composite.addDouble((Double) value);
                }
                else if (cl.equals(Float.class))
                {
                    composite.addFloat((Float) value);
                }
                else if (cl.equals(Boolean.class))
                {
                    composite.addBoolean((Boolean) value);
                }
                else if (cl.equals(String.class))
                {
                    composite.addUTF8((String) value);
                }
                else if (cl.equals(UUID.class))
                {
                    composite.addUuid((UUID) value);
                }
                else if (cl.equals(byte[].class))
                {
                    composite.addByteArray((byte[]) value);
                }
            }
            catch (PropertyAccessException ex)
            {
                Logger.getLogger(Composite.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        return composite;
    }

    public Composite addBoolean(Boolean value)
    {
        parts.add(value);
        builder.addBoolean(value);
        return this;
    }

    public Composite addBoolean(boolean value)
    {
        parts.add(value);
        builder.addBoolean(value);
        return this;
    }

    public Composite addByte(Byte value)
    {
        parts.add(value);
        builder.addByte(value);
        return this;
    }

    public Composite addByte(byte value)
    {
        parts.add(value);
        builder.addByte(value);
        return this;
    }

    public Composite addByteArray(byte[] value)
    {
        parts.add(value);
        builder.addByteArray(value);
        return this;
    }

    public Composite addByteBuffer(ByteBuffer value)
    {
        parts.add(value);
        builder.addByteBuffer(value);
        return this;
    }

    public Composite addBytes(Bytes value)
    {
        parts.add(value);
        builder.addBytes(value);
        return this;
    }

    public Composite addChar(Character value)
    {
        parts.add(value);
        builder.addChar(value);
        return this;
    }

    public Composite addChar(char value)
    {
        parts.add(value);
        builder.addChar(value);
        return this;
    }

    public Composite addDouble(Double value)
    {
        parts.add(value);
        builder.addDouble(value);
        return this;
    }

    public Composite addDouble(double value)
    {
        parts.add(value);
        builder.addDouble(value);
        return this;
    }

    public Composite addFloat(Float value)
    {
        parts.add(value);
        builder.addFloat(value);
        return this;
    }

    public Composite addFloat(float value)
    {
        parts.add(value);
        builder.addFloat(value);
        return this;
    }

    public Composite addInt(Integer value)
    {
        parts.add(value);
        builder.addInt(value);
        return this;
    }

    public Composite addInt(int value)
    {
        parts.add(value);
        builder.addInt(value);
        return this;
    }

    public Composite addLong(Long value)
    {
        parts.add(value);
        builder.addLong(value);
        return this;
    }

    public Composite addLong(long value)
    {
        parts.add(value);
        builder.addLong(value);
        return this;
    }

    public Composite addShort(Short value)
    {
        parts.add(value);
        builder.addShort(value);
        return this;
    }

    public Composite addShort(short value)
    {
        parts.add(value);
        builder.addShort(value);
        return this;
    }

    public Composite addTimeUuid(com.eaio.uuid.UUID value)
    {
        parts.add(value);
        builder.addTimeUuid(value);
        return this;
    }

    public Composite addUTF8(String value)
    {
        parts.add(value);
        builder.addUTF8(value);
        return this;
    }

    public Composite addUuid(UUID value)
    {
        parts.add(value);
        builder.addUuid(value);
        return this;
    }

    public Composite addUuid(long msb, long lsb)
    {
        parts.add(new UUID(msb, lsb));
        builder.addUuid(msb, lsb);
        return this;
    }

    public Bytes build()
    {
        return builder.build();
    }

    public byte[] serialize()
    {
        return builder.build().toByteArray();
    }

    public ByteBuffer serializeToByteBuffer()
    {
        return builder.build().getBytes();
    }

    public static Composite parse(Bytes bytes, Field field)
    {
        return parse(bytes.toByteArray(), field);
    }

    public static Composite parse(byte[] bytes, Field field)
    {
        List<byte[]> ckeyList = null;

        try
        {
            ckeyList = CompositeType.parse(bytes);
        }
        catch (IllegalArgumentException ex)
        {
            throw new MarshalException(ex.getMessage());
        }

        Composite composite = new Composite();

        Class[] compositeFieldTypes = getCompositeFieldTypes(field);

        if (compositeFieldTypes == null || ckeyList.size() != compositeFieldTypes.length)
        {
            throw new MarshalException("Supplied byte array did not parse correctly "
                    + "or composite field not annotated correctly.");
        }

        for (int i = 0; i < ckeyList.size(); i++)
        {
            try
            {
                byte[] part = ckeyList.get(i);
                Class cl = compositeFieldTypes[i];
                PropertyAccessor accessor = PropertyAccessorFactory.getPropertyAccessor(cl);
                Object value = accessor.fromBytes(part);

                if (cl.equals(Long.class))
                {
                    composite.addLong((Long) value);
                }
                else if (cl.equals(Integer.class))
                {
                    composite.addInt((Integer) value);
                }
                else if (cl.equals(Double.class))
                {
                    composite.addDouble((Double) value);
                }
                else if (cl.equals(Float.class))
                {
                    composite.addFloat((Float) value);
                }
                else if (cl.equals(Boolean.class))
                {
                    composite.addBoolean((Boolean) value);
                }
                else if (cl.equals(String.class))
                {
                    composite.addUTF8((String) value);
                }
                else if (cl.equals(UUID.class))
                {
                    composite.addUuid((UUID) value);
                }
                else if (cl.equals(byte[].class))
                {
                    composite.addByteArray((byte[]) value);
                }
            }
            catch (PropertyAccessException ex)
            {
                Logger.getLogger(Composite.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        return composite;
    }

    private static Class[] getCompositeFieldTypes(Field field)
    {
        com.impetus.client.cassandra.pelops.composite.CompositeType ct = field
                .getAnnotation(com.impetus.client.cassandra.pelops.composite.CompositeType.class);
        return ct == null ? null : ct.parts();
    }

    public Iterator<Object> iterator()
    {
        return parts.iterator();
    }

    public List<Object> parts()
    {
        return parts;
    }

    /**
     * Compares composites one part at a time. Currently only composites composed
     * of Numbers, UUIDs, Strings, and any class implementing the Comparable
     * interface.
     * @param t
     * @return 
     */
    @Override
    public int compareTo(Composite t)
    {
        if (parts.size() < t.parts.size())
        {
            return -1;
        }
        else if (parts.size() > t.parts.size())
        {
            return 0;
        }

        int diff = 0;

        for (int i = 0; i < parts.size(); i++)
        {
            Object a = parts.get(i);
            Object b = t.parts.get(i);

            if (a.getClass().equals(b.getClass()))
            {
                if (a.getClass().isAssignableFrom(Number.class))
                {
                    diff += ((Number) a).doubleValue() - ((Number) b).doubleValue();
                }
                else if (a instanceof String)
                {
                    diff += ((String) a).compareTo((String) b);
                }
                else if (a instanceof UUID)
                {
                    diff += compareUuids((UUID) a, (UUID) b);
                }
                if (a.getClass().isAssignableFrom(Comparable.class))
                {
                    diff += ((Comparable) a).compareTo((Comparable) b);
                }
            }
            else
            {
                //?
            }
        }

        return diff;
    }

    /**
     * Compares uuids correctly, unlike java implementation of UUID.compare. NOTE:
     * This assumes UUIDs are time based version 1.
     * @param uuid1
     * @param uuid2
     * @return 
     */
    private int compareUuids(UUID uuid1, UUID uuid2)
    {
        int diff = compareULongs(uuid1.timestamp(), uuid2.timestamp());
        if (diff == 0)
        {
            // or if that won't work, by other bits lexically
            diff = compareULongs(uuid1.getLeastSignificantBits(), uuid2.getLeastSignificantBits());
        }

        return diff;
    }

    private static int compareULongs(long l1, long l2)
    {
        int diff = compareUInts((int) (l1 >> 32), (int) (l2 >> 32));
        if (diff == 0)
        {
            diff = compareUInts((int) l1, (int) l2);
        }
        return diff;
    }

    private static int compareUInts(int i1, int i2)
    {
        /*
         * bit messier due to java's insistence on signed values: if both have
         * same sign, normal comparison (by subtraction) works fine; but if
         * signs don't agree need to resolve differently
         */
        if (i1 < 0)
        {
            return (i2 < 0) ? (i1 - i2) : 1;
        }
        return (i2 < 0) ? -1 : (i1 - i2);
    }
}