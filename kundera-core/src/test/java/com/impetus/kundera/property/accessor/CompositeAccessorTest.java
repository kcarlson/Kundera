/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.impetus.kundera.property.accessor;

import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.complex.Composite;
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
    public void test() throws PropertyAccessException
    {

        UUID id = UUID.randomUUID();
        Long l = 1L;

        Composite composite1 = new Composite(id, l);

        CompositeAccessor accessor = new CompositeAccessor();

        String string = accessor.toString(composite1);

        Composite composite2 = accessor.fromString(string);

        assert composite1.equals(composite2);

    }
}
