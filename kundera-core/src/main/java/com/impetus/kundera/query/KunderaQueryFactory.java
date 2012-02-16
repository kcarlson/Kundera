/**
 * *****************************************************************************
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); * you may
 * not use this file except in compliance with the License. * You may obtain a
 * copy of the License at * * http://www.apache.org/licenses/LICENSE-2.0 * *
 * Unless required by applicable law or agreed to in writing, software *
 * distributed under the License is distributed on an "AS IS" BASIS, * WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. * See the
 * License for the specific language governing permissions and * limitations
 * under the License.
 * ****************************************************************************
 */
package com.impetus.kundera.query;

import com.impetus.kundera.client.ClientType;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author kcarlson
 */
public class KunderaQueryFactory
{

    private static Log log = LogFactory.getLog(KunderaQueryFactory.class);

    private final ClientType clientType;

    KunderaQueryFactory(ClientType clientType)
    {
        this.clientType = clientType;
    }

    KunderaQuery build(String query, String[] persistenceUnits)
    {
        KunderaQuery kQuery;
        if (clientType.isNative())
        {
            kQuery = getNativeQuery(query, persistenceUnits);
            KunderaQueryParser parser = getNativeQueryParser(kQuery, query);
            parser.parse();
            kQuery.postParsingInit();
        }
        else
        {
            kQuery = new KunderaJpaQuery(persistenceUnits);
            KunderaJpaQueryParser parser = new KunderaJpaQueryParser(kQuery, query);
            parser.parse();
            kQuery.postParsingInit();
        }

        return kQuery;
    }

    private KunderaQuery getNativeQuery(String query, String[] persistenceUnits)
    {
        KunderaQuery kunderaQuery = null;
        try
        {
            Class clazz;
            switch (clientType)
            {
            case PELOPS:
                clazz = Class.forName("com.impetus.client.cassandra.query.KunderaNativeQuery");
                break;
            case THRIFT:
                clazz = Class.forName("com.impetus.client.cassandra.query.KunderaNativeQuery");
                break;
            default:
                throw new ClassNotFoundException("Invalid Client type" + clientType);
            }

            @SuppressWarnings("rawtypes")
            Constructor constructor = clazz.getConstructor(String[].class);

            kunderaQuery = (KunderaQuery) constructor.newInstance((Object) persistenceUnits);
        }
        catch (NoSuchMethodException ex)
        {
            log.fatal(null, ex);
        }
        catch (SecurityException ex)
        {
            log.fatal(null, ex);
        }
        catch (InstantiationException ex)
        {
            log.fatal(null, ex);
        }
        catch (IllegalAccessException ex)
        {
            log.fatal(null, ex);
        }
        catch (IllegalArgumentException ex)
        {
            log.fatal(null, ex);
        }
        catch (InvocationTargetException ex)
        {
            log.fatal(null, ex);
        }
        catch (ClassNotFoundException ex)
        {
            log.fatal(null, ex);
        }

        return kunderaQuery;
    }

    private KunderaQueryParser getNativeQueryParser(KunderaQuery kunderaQuery, String query)
    {
        KunderaQueryParser parser = null;
        try
        {
            Class clazz;
            switch (clientType)
            {
            case PELOPS:
                clazz = Class.forName("com.impetus.client.cassandra.query.KunderaNativeQueryParser");
                break;
            case THRIFT:
                clazz = Class.forName("com.impetus.client.cassandra.query.KunderaNativeQueryParser");
                break;
            default:
                throw new ClassNotFoundException("Invalid Client type" + clientType);
            }

            @SuppressWarnings("rawtypes")
            Constructor constructor = clazz.getConstructor(KunderaQuery.class, String.class);

            parser = (KunderaQueryParser) constructor.newInstance(kunderaQuery, query);
        }
        catch (NoSuchMethodException ex)
        {
            log.fatal(null, ex);
        }
        catch (SecurityException ex)
        {
            log.fatal(null, ex);
        }
        catch (InstantiationException ex)
        {
            log.fatal(null, ex);
        }
        catch (IllegalAccessException ex)
        {
            log.fatal(null, ex);
        }
        catch (IllegalArgumentException ex)
        {
            log.fatal(null, ex);
        }
        catch (InvocationTargetException ex)
        {
            log.fatal(null, ex);
        }
        catch (ClassNotFoundException ex)
        {
            log.fatal(null, ex);
        }

        return parser;
    }
}
