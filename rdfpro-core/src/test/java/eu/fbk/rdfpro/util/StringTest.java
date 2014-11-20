package eu.fbk.rdfpro.util;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class StringTest {

    @Test
    public void test() {

        final List<String> list = new ArrayList<String>();
        final List<char[]> list2 = new ArrayList<char[]>();
        for (int i = 0; i < 1000; ++i) {
            final String s = "this is " + Math.random() + " times a string with " + Math.random()
                    + " characters";
            list.add(s);
            final int len = s.length();
            final char[] cb = new char[len];
            s.getChars(0, len, cb, 0);
            list2.add(cb);
        }

        final long ts0 = System.nanoTime();

        int sum = 0;
        for (int j = 0; j < 100000; ++j) {
            for (final char[] s : list2) {
                final int len = s.length;
                for (int i = 0; i < len; ++i) {
                    sum += s[i] * j;
                }
                // for (int i = 0; i < len; ++i) {
                // sum += s.charAt(i) * j;
                // }
            }
        }

        final long ts1 = System.nanoTime();

        System.out.println(sum + " " + (ts1 - ts0) / 1000000);
    }
}
