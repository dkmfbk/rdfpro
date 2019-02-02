package eu.fbk.rdfpro.util;

public class Exceptions {

    public static void throwIfUnchecked(final Throwable ex) {
        if (ex instanceof Error) {
            throw (Error) ex;
        } else if (ex instanceof RuntimeException) {
            throw (RuntimeException) ex;
        }
    }

}
