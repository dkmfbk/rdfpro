package eu.fbk.rdfpro.rules.util;

import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.XMLGregorianCalendar;

import com.google.common.base.Strings;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import eu.fbk.rdfpro.util.Statements;

public class StringIndexTest {

    @Test
    public void test2() {
        final float x = 17.625f;
        System.out.println(Integer.toHexString(Float.floatToIntBits(x)));

        final XMLGregorianCalendar c = Statements.DATATYPE_FACTORY.newXMLGregorianCalendar();
        c.setDay(21);
        c.setMonth(11);
        c.setYear(2001);
        c.setHour(0);
        c.setMinute(0);
        c.setSecond(0);
        System.out.println(c.toXMLFormat());
    }

    @Ignore
    @Test
    public void test() {
        final String small = "small";
        final String large = Strings.repeat("large", 100);
        final StringIndex index = new StringIndex();
        Assert.assertEquals(0, index.size());
        index.put("test");
        Assert.assertEquals(1, index.size());
        for (final String string : new String[] { small, large }) {
            Assert.assertFalse(index.contains(string));
            final int id = index.put(string);
            Assert.assertTrue(index.contains(string));
            Assert.assertEquals(string, index.get(id));
            Assert.assertTrue(index.equals(id, string));
            Assert.assertFalse(index.equals(id, "something_else"));
            Assert.assertEquals(string.length(), index.length(id));
        }
    }

}
