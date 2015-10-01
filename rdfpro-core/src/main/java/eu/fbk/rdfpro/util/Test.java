package eu.fbk.rdfpro.util;

import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilderFactory;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashingInputStream;
import com.google.common.io.CharStreams;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class Test {

    private static final Pattern PATTERN = Pattern.compile(Pattern.quote("resource=\"")
            + "[a-zA-Z0-9]+" + Pattern.quote("\""));

    public static void main(final String... args) throws Throwable {

        final long ts = System.currentTimeMillis();
        try (Writer w = IO.utf8Writer(IO.buffer(IO.write("extref_" + ts + ".txt")))) {
            final byte[][] buffers = new byte[args.length][];
            final Set<String> allNames = Sets.newHashSet();
            final Set<String> allNamesRaw = Sets.newHashSet();
            Arrays.sort(args);
            final AtomicLong totalSize = new AtomicLong();
            final ExecutorService service = Executors.newFixedThreadPool(Environment.getCores());
            try {
                for (int i = 0; i < args.length; ++i) {
                    final int index = i;
                    final String arg = args[i];
                    service.execute(new Runnable() {

                        @Override
                        public void run() {
                            try (Reader in = IO.utf8Reader(IO.buffer(IO.read(arg)))) {
                                final String s = CharStreams.toString(in);
                                final Matcher m = PATTERN.matcher(s);
                                final Set<String> names = Sets.newHashSet();
                                while (m.find()) {
                                    names.add(m.group());
                                }
                                synchronized (allNamesRaw) {
                                    allNamesRaw.addAll(names);
                                }
                            } catch (final Throwable ex) {
                                ex.printStackTrace();
                            }
                            try (InputStream in = IO.read(arg)) {
                                final HashingInputStream his = new HashingInputStream(Hashing
                                        .md5(), in);
                                int len;
                                final Set<String> names = Sets.newHashSet();
                                final Document doc;
                                synchronized (Test.class) {
                                    final DocumentBuilderFactory dbf = DocumentBuilderFactory
                                            .newInstance();
                                    dbf.setFeature(
                                            "http://apache.org/xml/features/dom/defer-node-expansion",
                                            false);
                                    doc = dbf.newDocumentBuilder().parse(his);
                                }
                                final NodeList l = doc.getElementsByTagName("externalRef");
                                for (int i = 0; i < l.getLength(); ++i) {
                                    names.add(((Element) l.item(i)).getAttribute("resource"));
                                }
                                len = l.getLength();
                                synchronized (allNames) {
                                    allNames.addAll(names);
                                }
                                totalSize.addAndGet(len);
                                buffers[index] = his.hash().asBytes();
                            } catch (final Throwable ex) {
                                ex.printStackTrace();
                            }
                        }

                    });
                }
            } finally {
                service.shutdown();
                service.awaitTermination(60, TimeUnit.MINUTES);
                service.shutdownNow();
            }
            final Hasher hasher = Hashing.md5().newHasher();
            for (int i = 0; i < args.length; ++i) {
                hasher.putBytes(buffers[i]);
            }
            final String code = hasher.hash().toString();
            for (final String name : Ordering.natural().immutableSortedCopy(allNames)) {
                w.write(name + "\n");
            }
            try (Writer w2 = IO.utf8Writer(IO.buffer(IO.write("extref_" + ts + ".raw")))) {
                for (final String name : Ordering.natural().immutableSortedCopy(allNamesRaw)) {
                    w2.write(name + "\n");
                }
            }
            System.out.println(code + " - " + args.length + " files, " + totalSize + " bytes, "
                    + (System.currentTimeMillis() - ts) + " ms");
        }
    }

}
