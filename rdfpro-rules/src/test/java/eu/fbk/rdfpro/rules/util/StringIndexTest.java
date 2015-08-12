package eu.fbk.rdfpro.rules.util;

import com.google.common.base.Strings;

import org.junit.Assert;
import org.junit.Test;

public class StringIndexTest {

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
