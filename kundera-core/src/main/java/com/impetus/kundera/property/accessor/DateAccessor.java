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
package com.impetus.kundera.property.accessor;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import com.impetus.kundera.Constants;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessor;
import org.scale7.cassandra.pelops.Bytes;

/**
 * The Class DateAccessor.
 * 
 * @author animesh.kumar
 */
public class DateAccessor implements PropertyAccessor<Date>
{

    /** The Constant DATE_FORMATTER. */
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("dd MMM yyyy HH:mm:ss:S Z",
            Locale.ENGLISH);

    /** The patterns. */
    private static List<String> patterns = new ArrayList<String>(15);

    static
    {
        patterns.add("E MMM dd HH:mm:ss z yyyy");
        patterns.add("dd MMM yyyy HH:mm:ss:SSS");
        patterns.add("dd MMM yyyy H:mm:ss:SSS");
        patterns.add("MM-dd-yyyy HH:mm:ss:SSS");
        patterns.add("MM/dd/yyyy HH:mm:ss:SSS");
        patterns.add("dd/MM/yyyy HH:mm:ss:SSS");
        patterns.add("dd-MM-yyyy HH:mm:ss:SSS");
        patterns.add("MMM/dd/yyyy HH:mm:ss:SSS");
        patterns.add("MMM-dd-yyyy HH:mm:ss:SSS");
        patterns.add("dd-MMM-yyyy HH:mm:ss:SSS");
        patterns.add("MM-dd-yyyy H:mm:ss:SSS");
        patterns.add("MM/dd/yyyy H:mm:ss:SSS");
        patterns.add("dd/MM/yyyy H:mm:ss:SSS");
        patterns.add("dd-MM-yyyy H:mm:ss:SSS");
        patterns.add("MMM/dd/yyyy H:mm:ss:SSS");
        patterns.add("MMM-dd-yyyy H:mm:ss:SSS");
        patterns.add("dd-MMM-yyyy H:mm:ss:SSS");
        patterns.add("MM-dd-yyyy HH:mm:ss");
        patterns.add("MM/dd/yyyy HH:mm:ss");
        patterns.add("dd/MM/yyyy HH:mm:ss");
        patterns.add("dd-MM-yyyy HH:mm:ss");
        patterns.add("MMM/dd/yyyy HH:mm:ss");
        patterns.add("MMM-dd-yyyy HH:mm:ss");
        patterns.add("dd-MMM-yyyy HH:mm:ss");
        patterns.add("MM-dd-yyyy H:mm:ss");
        patterns.add("MM/dd/yyyy H:mm:ss");
        patterns.add("dd/MM/yyyy H:mm:ss");
        patterns.add("dd-MM-yyyy H:mm:ss");
        patterns.add("MMM/dd/yyyy H:mm:ss");
        patterns.add("MMM-dd-yyyy H:mm:ss");
        patterns.add("dd-MMM-yyyy H:mm:ss");

        patterns.add("MM-dd-yyyy");
        patterns.add("MM/dd/yyyy");
        patterns.add("dd/MM/yyyy");
        patterns.add("dd-MM-yyyy");
        patterns.add("MMM/dd/yyyy");
        patterns.add("MMM-dd-yyyy");
        patterns.add("dd-MMM-yyyy");
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.property.PropertyAccessor#fromBytes(byte[])
     */
    @Override
    public final Date fromBytes(byte[] bytes) throws PropertyAccessException
    {
        try
        {
            // return DATE_FORMATTER.parse(new String(bytes,
            // Constants.ENCODING));
            return getDateByPattern(new String(bytes, Constants.ENCODING));
        }
        catch (Exception e)
        {
            throw new PropertyAccessException(e.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.property.PropertyAccessor#toBytes(java.lang.Object)
     */
    @Override
    public final byte[] toBytes(Object date) throws PropertyAccessException
    {
        try
        {
            return DATE_FORMATTER.format(((Date) date)).getBytes(Constants.ENCODING);
            //return Bytes.fromLong(((Date)date).getTime()).toByteArray();
        }
        catch (Exception e)
        {
            throw new PropertyAccessException(e.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.property.PropertyAccessor#toString(java.lang.Object)
     */
    @Override
    public final String toString(Object object)
    {
        return object.toString();
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.property.PropertyAccessor#fromString(java.lang.String)
     */
    @Override
    public Date fromString(String s) throws PropertyAccessException
    {
        try
        {
            Date d = getDateByPattern(s);
            return d;
        }
        catch (NumberFormatException e)
        {
            throw new PropertyAccessException(e.getMessage());
        }
    }

    /**
     * Get Date from given below formats.
     *
     * @param date Date pattern
     * @return date instance
     * @throws PropertyAccessException throws only if invalid format is supplied.
     */
    public static Date getDateByPattern(String date) throws PropertyAccessException
    {
        for (String p : patterns)
        {
            try
            {
                DateFormat formatter = new SimpleDateFormat(p);
                Date dt = formatter.parse(date);
                return dt;
            }
            catch (IllegalArgumentException iae)
            {
                // Do nothing.
                // move to next pattern.
            }
            catch (ParseException e)
            {
                // Do nothing.
                // move to next pattern.
            }

        }

        throw new PropertyAccessException("Required Date format is not supported!" + date);
    }

    /**
     * Just to verify with supported types of date pattern. Get Date from given
     * below formats
     * 
     * @param date
     *            Date pattern
     * @return date instance
     * @throws PropertyAccessException
     *             throws only if invalid format is supplied.
     */
    public static String getFormattedObect(String date) throws PropertyAccessException
    {
        return getDateByPattern(date).toString();
    }
}