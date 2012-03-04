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
import java.lang.Object;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.types.CompositeType;

public class Composite implements Serializable
{
    /**
     * Component id for boolean values
     */
    public final static int COMPONENT_BOOL = 2;

    /**
     * Component id for long values
     */
    public final static int COMPONENT_LONG = 3;

    /**
     * Component id for real values
     */
    public final static int COMPONENT_REAL = 4;

    /**
     * Component id for timeuuid values
     */
    public final static int COMPONENT_TIMEUUID = 5;

    /**
     * Component id for lexical uuid values
     */
    public final static int COMPONENT_LEXICALUUID = 6;

    /**
     * Component id for ascii character strings
     */
    public final static int COMPONENT_ASCII = 7;

    /**
     * Component id for utf8 encoded strings
     */
    public final static int COMPONENT_UTF8 = 8;

    /**
     * Component id for byte array values
     */
    public final static int COMPONENT_BYTES = 9;

    /**
     * Component id for place holder matching the maximum possible value.
     */
    public final static int COMPONENT_MAXIMUM = 255;

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
        if (parts.isEmpty())
        {
            return "";
        }

        Iterator<Object> iter = parts.iterator();
        StringBuilder sb = new StringBuilder();

        while (iter.hasNext())
        {
            Object o = iter.next();
            sb.append(o.toString());
            if (o instanceof Long)
            {
                sb.append(";").append(COMPONENT_LONG);
            }
            else if (o instanceof Integer)
            {
                sb.append(";").append(COMPONENT_LONG);
            }
            else if (o instanceof Double)
            {
                sb.append(";").append(COMPONENT_REAL);
            }
            else if (o instanceof Float)
            {
                sb.append(";").append(COMPONENT_REAL);
            }
            else if (o instanceof Boolean)
            {
                sb.append(";").append(COMPONENT_BOOL);
            }
            else if (o instanceof String)
            {
                sb.append(";").append(COMPONENT_UTF8);
            }
            else if (o instanceof UUID)
            {
                sb.append(";").append(COMPONENT_LEXICALUUID);
            }
            else if (o instanceof byte[])
            {
                sb.append(";").append(COMPONENT_BYTES);
            }

            sb.append(",");
        }
        return sb.toString().substring(0, sb.length() - 1);
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

    public static Composite fromString(String s)
    {
        StringTokenizer st = new StringTokenizer(s, ",");
        Composite composite = new Composite();

        while (st.hasMoreTokens())
        {
            String token = st.nextToken();
            String[] subTokens = token.split(";");

            if (subTokens.length != 2)
            {
                throw new MarshalException("Not a string-serialized composite type: " + s);
            }

            token = subTokens[0];
            int type = Integer.parseInt(subTokens[1]);

            switch (type)
            {
            case COMPONENT_BYTES:
                byte b = Byte.decode(token);
                composite.addByte(b);
                break;
            case COMPONENT_ASCII:
                composite.addUTF8(token);
                break;

            case COMPONENT_UTF8:
                composite.addUTF8(token);
                break;

            case COMPONENT_BOOL:
                boolean bool = Boolean.parseBoolean(token);
                composite.addBoolean(bool);
                break;

            case COMPONENT_LONG:
                long lng = Long.parseLong(token);
                composite.addLong(lng);
                break;

            case COMPONENT_REAL:
                double dbl = Double.parseDouble(token);
                composite.addDouble(dbl);
                break;

            case COMPONENT_LEXICALUUID:
                UUID lexicalUuid = UUID.fromString(token);
                composite.addUuid(lexicalUuid);
                break;

            case COMPONENT_TIMEUUID:
                UUID timeUuid = UUID.fromString(token);
                composite.addUuid(timeUuid);
                break;

            default:
                throw new MarshalException("Unknown embedded type: " + type);
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
        List<byte[]> ckeyList = CompositeType.parse(bytes);
        Composite composite = new Composite();

        Class[] compositeFieldTypes = getCompositeFieldTypes(field);

        if (ckeyList.size() != compositeFieldTypes.length)
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
        return ct.parts();
    }

    public Iterator<Object> iterator()
    {
        return parts.iterator();
    }

    public List<Object> parts()
    {
        return parts;
    }
}