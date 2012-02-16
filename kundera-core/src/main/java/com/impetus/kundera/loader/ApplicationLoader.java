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
package com.impetus.kundera.loader;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class ApplicationLoader.
 *
 * @author amresh.singh
 */
public class ApplicationLoader implements Loader
{

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(ApplicationLoader.class);

    /** The application loaders. */
    List<ApplicationLoader> applicationLoaders = new ArrayList<ApplicationLoader>();

    /* (non-Javadoc)
     * @see com.impetus.kundera.loader.Loader#load(java.lang.String[])
     */
    @Override
    public void load(String... persistenceUnits)
    {
        log.debug("Loading User Application...");

        applicationLoaders.add(new PersistenceUnitLoader());
        applicationLoaders.add(new MetamodelLoader());

        for (ApplicationLoader appLoader : applicationLoaders)
        {
            appLoader.load(persistenceUnits);
        }
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.loader.Loader#unload(java.lang.String[])
     */
    @Override
    public void unload(String... persistenceUnits)
    {
        // TODO Auto-generated method stub

    }

}
