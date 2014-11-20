package eu.fbk.rdfpro.util;

import org.junit.Test;

public class OptionsTest {

    @Test
    public void test() {
        final String args = "a1 -s s0 a2 -b1 a3 -m m0 m1 m2 -b2 a4 -b3";
        final Options options = Options.parse("-s|--slong!,-b1,-b2,-b3,-m*", args.split(" "));
        System.out.println(options);
    }

}
