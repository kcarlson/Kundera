/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.impetus.client.junit;

import com.impetus.client.cassandra.pelops.composite.Composite;
import com.impetus.client.cassandra.pelops.composite.CompositeAccessor;
import com.impetus.client.cassandra.pelops.composite.CompositeType;
import com.impetus.kundera.property.PropertyAccessException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.UUID;
import org.junit.*;

/**
 *
 * @author kcarlson
 */
public class CompositeAccessorTest
{

    public CompositeAccessorTest()
    {
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
    }

    @AfterClass
    public static void tearDownClass() throws Exception
    {
    }

    @Before
    public void setUp()
    {
    }

    @After
    public void tearDown()
    {
    }

    @Test
    public void test() throws PropertyAccessException, NoSuchFieldException
    {

        UUID id = UUID.randomUUID();
        Long l = 1L;

        Composite composite1 = new Composite();
        composite1.addUuid(id).addLong(l);

        CompositeAccessor accessor = new CompositeAccessor();

        String string = accessor.toString(composite1);

        Composite composite2 = accessor.fromString(string);

        assert Arrays.equals(composite1.serialize(), composite2.serialize());

        byte[] bytes = accessor.toBytes(composite1);

        TestEntity entity = new TestEntity();
        entity.composite = composite1;

        Field field = entity.getClass().getDeclaredField("composite");

        Composite composite3 = accessor.fromBytes(bytes, field);

        assert Arrays.equals(composite1.serialize(), composite3.serialize());

    }
}

class TestEntity
{
    @CompositeType(parts = { UUID.class, Long.class })
    Composite composite;
}
