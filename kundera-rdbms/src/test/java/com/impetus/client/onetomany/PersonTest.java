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
package com.impetus.client.onetomany;

import java.util.HashSet;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.Test;

import com.impetus.client.onetoone.OTONPerson;

/**
 * @author vivek.mishra
 *
 */
public class PersonTest
{

//    @Test
    public void testPersist()
    {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("testHibernate,kcassandra");
        
        EntityManager em = emf.createEntityManager();
        em.persist(prepareObject());
        emf.close(); 
    }

    /**
     * @return
     */
    private Object prepareObject()
    {
        OTMNPerson person = new OTMNPerson();
        person.setPersonId("omp");
        person.setPersonName("omVivs");
        OTMAddress address = new OTMAddress();
        address.setAddressId("oma");
        address.setStreet("sadak");
        
        Set<OTMAddress> addresses = new HashSet<OTMAddress>(1);
        addresses.add(address);
        person.setAddresses(addresses);
        return person;
    }


    @Test
    public void testFindById()
    {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("testHibernate,kcassandra");
        
        EntityManager em = emf.createEntityManager();
        OTMNPerson obj = em.find(OTMNPerson.class, "omp");
        OTMAddress address =  obj.getAddresses().iterator().next();
        System.out.println(obj.getClass());
    }
}
