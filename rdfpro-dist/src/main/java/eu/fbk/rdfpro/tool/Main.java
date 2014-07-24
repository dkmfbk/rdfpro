/*
 * RDFpro - An extensible tool for building stream-oriented RDF processing libraries.
 * 
 * Written in 2014 by Francesco Corcoglioniti <francesco.corcoglioniti@gmail.com> with support by
 * Marco Rospocher, Marco Amadori and Michele Mostarda.
 * 
 * To the extent possible under law, the author has dedicated all copyright and related and
 * neighboring rights to this software to the public domain worldwide. This software is
 * distributed without any warranty.
 * 
 * You should have received a copy of the CC0 Public Domain Dedication along with this software.
 * If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package eu.fbk.rdfpro.tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Properties;

import org.openrdf.rio.RDFHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

import eu.fbk.rdfpro.RDFProcessor;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(final String... args) {

        try {
            Class.forName("eu.fbk.rdfp.tql.TQL");
        } catch (final Throwable ex) {
            // ignore - TQL will not be supported
        }

        try {
            Class.forName("eu.fbk.rdfp.GeonamesRDF");
        } catch (final Throwable ex) {
            // ignore - Geonames format will not be supported
        }

        boolean showHelp = false;
        boolean showVersion = false;
        boolean verbose = false;

        int index = 0;
        while (index < args.length) {
            final String arg = args[index];
            if (arg.startsWith("{") || arg.startsWith("@") || !arg.startsWith("-")) {
                break;
            }
            showHelp |= arg.equals("-h");
            showVersion |= arg.equals("-v");
            verbose |= arg.equals("-V");
            ++index;
        }
        showHelp |= index == args.length;

        if (verbose) {
            ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger("eu.fbk.rdfp"))
                    .setLevel(Level.DEBUG);
        }

        if (showVersion) {
            String version = "development version";
            final URL url = RDFProcessor.class.getClassLoader().getResource(
                    "META-INF/maven/eu.fbk.rdfp/rdfp-dist/pom.properties");
            if (url != null) {
                try {
                    final InputStream stream = url.openStream();
                    try {
                        final Properties properties = new Properties();
                        properties.load(stream);
                        version = properties.getProperty("version").trim();
                    } finally {
                        stream.close();
                    }

                } catch (final IOException ex) {
                    version = "unknown version";
                }
            }
            System.out.println(String.format("RDF Processor Tool (RDFP) %s\nJava %s bit (%s) %s\n"
                    + "This is free and unencumbered software released into the public domain",
                    version, System.getProperty("sun.arch.data.model"),
                    System.getProperty("java.vendor"), System.getProperty("java.version")));
            System.exit(0);
        }

        if (showHelp) {
            final URL url = RDFProcessor.class.getResource("help");
            try {
                final BufferedReader reader = new BufferedReader(new InputStreamReader(
                        url.openStream(), Charset.forName("UTF-8")));
                try {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        System.out.println(line);
                    }
                } finally {
                    reader.close();
                }
            } catch (final Throwable ex) {
                System.err.println("EXECUTION FAILED. " + ex.getMessage() + "\n");
                ex.printStackTrace();
                System.exit(3);
            }
            System.exit(0);
        }

        RDFProcessor processor = null;
        try {
            processor = RDFProcessor.parse(Arrays.copyOfRange(args, index, args.length));
        } catch (final IllegalArgumentException ex) {
            System.err.println("INVOCATION ERROR. " + ex.getMessage() + "\n");
            System.exit(1);
        }

        try {
            final long ts = System.currentTimeMillis();
            final RDFHandler handler = processor.getHandler();
            final int repetitions = processor.getExtraPasses() + 1;
            for (int i = 0; i < repetitions; ++i) {
                if (repetitions > 1) {
                    LOGGER.info("Pass {} of {}", i + 1, repetitions);
                }
                handler.startRDF();
                handler.endRDF();
            }
            LOGGER.info("Done in {} s", (System.currentTimeMillis() - ts) / 1000);
            System.exit(0);

        } catch (final Throwable ex) {
            System.err.println("EXECUTION FAILED. " + ex.getMessage() + "\n");
            ex.printStackTrace();
            System.exit(2);
        }
    }

}
