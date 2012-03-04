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
package com.impetus.client.cassandra.pelops;

import com.impetus.client.cassandra.pelops.composite.Composite;
import com.impetus.client.cassandra.pelops.composite.MarshalException;
import com.impetus.kundera.Constants;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.scale7.cassandra.pelops.Bytes;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * All methods in this utilities class take UUIDs into consideration when
 * converting.
 * @author kcarlson
 */
public class ByteUtils
{
    /** log for this class. */
    private static Log log = LogFactory.getLog(ByteUtils.class);

    /**
     * Converts a string to a Bytes object taking into consideration that the
     * string may be a UUID.
     * @param str
     * @return 
     */
    public static Bytes stringToBytes(String str, Field field)
    {
        try
        {
            UUID uuid = UUID.fromString(str);
            return Bytes.fromUuid(uuid);
        }
        catch (IllegalArgumentException ex)
        {
            try
            {
                Composite composite = Composite.fromString(str, field);
                return Bytes.fromByteBuffer(composite.serializeToByteBuffer());
            }
            catch (MarshalException ex2)
            {
                return Bytes.fromByteArray(str.getBytes());
            }
        }
    }

    /**
     * Converts a Bytes object into a string taking into consideration that the
     * Bytes object may be a UUID.
     * @param bytes
     * @return 
     */
    public static String bytesToString(Bytes bytes, Field field)
    {
        try
        {
            // Check that length is 32 first. Sometimes if using two or more UUIDs
            // as a composite key will still convert to a UUID string which
            // is wrong.
            if (bytes.length() != 16)
            {
                return Bytes.toUTF8(bytes.toByteArray());
            }
            UUID uuid = bytes.toUuid();
            return uuid.toString();
        }
        catch (IllegalStateException ex)
        {
            try
            {
                Composite composite = Composite.parse(bytes, field);
                return composite.toString();
            }
            catch (MarshalException ex2)
            {
                return Bytes.toUTF8(bytes.toByteArray());
            }
        }
    }

    /**
     * Converts a byte array into a string taking into consideration that the
     * byte array may be a UUID.
     * @param byteArray
     * @return 
     */
    public static String byteArrayToString(byte[] byteArray, Field field)
    {
        return bytesToString(Bytes.fromByteArray(byteArray), field);
    }

    /**
     * COnverts a string to a byte array taking into consideration that the
     * string may be a UUID.
     * @param str
     * @return
     * @throws Exception 
     */
    public static byte[] stringToByteArray(String str, Field field) throws Exception
    {
        try
        {
            UUID uuid = UUID.fromString(((String) str));
            return Bytes.fromUuid(uuid).toByteArray();
        }
        catch (IllegalArgumentException ex)
        {
            try
            {
                Composite composite = Composite.fromString(str, field);
                return composite.serialize();
            }
            catch (MarshalException ex2)
            {
                try
                {
                    return str.getBytes(Constants.ENCODING);
                }
                catch (UnsupportedEncodingException ex1)
                {
                    throw new Exception(ex1.getMessage());
                }
            }

        }
    }

    /**
     * Converts a string into a ByteBuffer object taking into consideration that the
     * string may be a UUID.
     * @param str
     * @return 
     */
    public static ByteBuffer stringToByteBuffer(String str, Field field)
    {
        try
        {
            UUID uuid = UUID.fromString(str);
            return ByteBuffer.wrap(Bytes.fromUuid(uuid).toByteArray());
        }
        catch (IllegalArgumentException ex1)
        {
            try
            {
                Composite composite = Composite.fromString(str, field);
                return composite.serializeToByteBuffer();
            }
            catch (MarshalException ex2)
            {
                return ByteBufferUtil.bytes(str);
            }
        }
    }

}
