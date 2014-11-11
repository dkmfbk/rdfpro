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
package eu.fbk.rdfpro;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class FilterProcessor extends RDFProcessor {

    private static final String[] EMPTY = new String[0];

    @Nullable
    private final ValueMatcher subjectDecisor;

    @Nullable
    private final ValueMatcher predicateDecisor;

    @Nullable
    private final ValueMatcher objectDecisor;

    @Nullable
    private final ValueMatcher contextDecisor;

    @Nullable
    private final ValueMatcher typeDecisor;

    @Nullable
    private final ValueReplacer subjectReplacer;

    @Nullable
    private final ValueReplacer predicateReplacer;

    @Nullable
    private final ValueReplacer objectReplacer;

    @Nullable
    private final ValueReplacer contextReplacer;

    @Nullable
    private final ValueReplacer typeReplacer;

    private final int numGroups;

    private final boolean keep;

    FilterProcessor(@Nullable final String matchSpec, @Nullable final String replaceSpec,
            final boolean keep) {

        final AtomicInteger numGroups = new AtomicInteger(0);

        // Match decisor.
        final Map<Character, Map<Character, StringMatcher>> m;
        m = new HashMap<Character, Map<Character, StringMatcher>>();
        if (matchSpec != null) {
            final List<String> tokens = tokenize(matchSpec);
            int index = 0;
            while (index < tokens.size()) {
                final String target = tokens.get(index++);
                final List<String> rules = new ArrayList<String>();
                while (index < tokens.size()) {
                    final String token = tokens.get(index);
                    if (!token.startsWith("+") && !token.startsWith("-")) {
                        break;
                    }
                    rules.add(token);
                    ++index;
                }
                index(m, target, new StringMatcher(rules, numGroups));
            }
        }

        this.subjectDecisor = !m.containsKey('s') ? null : new ValueMatcher(m.get('s'));
        this.predicateDecisor = !m.containsKey('p') ? null : new ValueMatcher(m.get('p'));
        this.objectDecisor = !m.containsKey('o') ? null : new ValueMatcher(m.get('o'));
        this.contextDecisor = !m.containsKey('c') ? null : new ValueMatcher(m.get('c'));
        this.typeDecisor = !m.containsKey('t') ? null : new ValueMatcher(m.get('t'));

        // Match replacer.
        final Map<Character, Map<Character, StringReplacer>> r;
        r = new HashMap<Character, Map<Character, StringReplacer>>();
        if (replaceSpec != null) {
            final List<String> tokens = tokenize(replaceSpec);
            for (int i = 0; i < tokens.size(); i += 2) {
                index(r, tokens.get(i), new StringReplacer(tokens.get(i + 1)));
            }
        }

        this.subjectReplacer = !r.containsKey('s') ? null : new ValueReplacer(r.get('s'));
        this.predicateReplacer = !r.containsKey('p') ? null : new ValueReplacer(r.get('p'));
        this.objectReplacer = !r.containsKey('o') ? null : new ValueReplacer(r.get('o'));
        this.contextReplacer = !r.containsKey('c') ? null : new ValueReplacer(r.get('c'));
        this.typeReplacer = !r.containsKey('t') ? null : new ValueReplacer(r.get('t'));

        this.numGroups = numGroups.get();
        this.keep = keep;
    }

    @Override
    public int getExtraPasses() {
        return 0;
    }

    @Override
    public RDFHandler getHandler(final RDFHandler handler) {
        return new Handler(handler);
    }

    private static List<String> tokenize(final String string) {

        final List<String> tokens = new ArrayList<String>();

        final StringBuilder builder = new StringBuilder();
        boolean escaped = false;
        char quote = 0;
        int start = -1;

        for (int i = 0; i < string.length(); ++i) {
            final char ch = string.charAt(i);
            final boolean ws = Character.isWhitespace(ch);
            if (ch == '\\' && !escaped && i < string.length() - 1) {
                final char ch2 = string.charAt(i + 1);
                if (ch2 == '\'' || ch2 == '\"' || ch2 == '|') {
                    escaped = true;
                    continue;
                }
            }
            if (start < 0) {
                if (!ws) {
                    start = i;
                    builder.setLength(0);
                    builder.append(ch);
                    if (ch == '\'' || ch == '\"' || ch == '|') {
                        quote = ch;
                    } else {
                        quote = 0;
                    }
                }
            } else {
                if (escaped || quote != 0 && ch != quote || quote == 0 && !ws) {
                    final boolean startQuoted = quote == 0 && i == start + 1
                            && (ch == '\'' || ch == '\"' || ch == '|')
                            && (string.charAt(start) == '+' || string.charAt(start) == '-');
                    if (startQuoted) {
                        quote = ch;
                    }
                    builder.append(ch);
                } else {
                    if (!ws) {
                        builder.append(ch);
                    }
                    tokens.add(builder.toString());
                    start = -1;
                    quote = 0;
                }
            }
            escaped = false;
        }

        if (start >= 0) {
            tokens.add(builder.toString());
        }

        return tokens;
    }

    private static <T> void index(final Map<Character, Map<Character, T>> map,
            final String target, final T object) {

        for (int j = 0; j < target.length(); ++j) {
            final char component = target.charAt(j);
            if (component == 's' || component == 'p' || component == 'o' || component == 'c'
                    || component == 't') {
                Map<Character, T> typeMap = map.get(component);
                if (typeMap == null) {
                    typeMap = new HashMap<Character, T>();
                    map.put(component, typeMap);
                }
                for (int k = 0; k < target.length(); ++k) {
                    final char type = target.charAt(k);
                    if (type == 'u' || type == 'b' || type == 'l' || type == '@' || type == '^') {
                        typeMap.put(type, object);
                    }
                }
            }
        }
    }

    private final class Handler implements RDFHandler, Closeable {

        private final RDFHandler handler;

        Handler(final RDFHandler handler) {
            this.handler = Util.checkNotNull(handler);
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            this.handler.startRDF();
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            this.handler.handleComment(comment);
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            this.handler.handleNamespace(prefix, uri);
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {

            Resource subj = statement.getSubject();
            URI pred = statement.getPredicate();
            Value obj = statement.getObject();
            Resource ctx = statement.getContext();

            final String[] groups = FilterProcessor.this.numGroups == 0 ? EMPTY
                    : new String[FilterProcessor.this.numGroups];

            boolean result = true;
            if (FilterProcessor.this.predicateDecisor != null) {
                result = FilterProcessor.this.predicateDecisor.match(pred, groups);
            }
            if (result && FilterProcessor.this.typeDecisor != null && pred.equals(RDF.TYPE)) {
                result = FilterProcessor.this.typeDecisor.match(obj, groups);
            }
            if (result && FilterProcessor.this.objectDecisor != null) {
                result = FilterProcessor.this.objectDecisor.match(obj, groups);
            }
            if (result && FilterProcessor.this.contextDecisor != null) {
                result = FilterProcessor.this.contextDecisor.match(ctx, groups);
            }
            if (result && FilterProcessor.this.subjectDecisor != null) {
                result = FilterProcessor.this.subjectDecisor.match(subj, groups);
            }

            if (!result) {
                if (FilterProcessor.this.keep) {
                    this.handler.handleStatement(statement);
                }
                return;
            }

            boolean changed = false;

            if (pred.equals(RDF.TYPE) && FilterProcessor.this.typeReplacer != null) {
                final Value oldObj = obj;
                obj = FilterProcessor.this.contextReplacer.replace(oldObj, groups);
                changed |= oldObj != obj;
            } else if (FilterProcessor.this.objectReplacer != null) {
                final Value oldObj = obj;
                obj = FilterProcessor.this.objectReplacer.replace(oldObj, groups);
                changed |= oldObj != obj;
            }

            if (FilterProcessor.this.predicateReplacer != null) {
                final URI oldPred = pred;
                pred = (URI) FilterProcessor.this.predicateReplacer.replace(oldPred, groups);
                changed |= oldPred != pred;
            }

            if (FilterProcessor.this.subjectReplacer != null) {
                final Resource oldSubj = subj;
                subj = (Resource) FilterProcessor.this.subjectReplacer.replace(oldSubj, groups);
                changed |= oldSubj != subj;
            }

            if (FilterProcessor.this.contextReplacer != null) {
                final Resource oldCtx = ctx;
                ctx = (Resource) FilterProcessor.this.contextReplacer.replace(oldCtx, groups);
                changed |= oldCtx != ctx;
            }

            final Statement stmt;
            if (!changed) {
                stmt = statement;
            } else if (ctx == null) {
                stmt = Util.FACTORY.createStatement(subj, pred, obj);
            } else {
                stmt = Util.FACTORY.createStatement(subj, pred, obj, ctx);
            }

            this.handler.handleStatement(stmt);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            this.handler.endRDF();
        }

        @Override
        public void close() throws IOException {
            Util.closeQuietly(this.handler);
        }

    }

    private static final class ValueMatcher {

        @Nullable
        private final StringMatcher uriDecisor;

        @Nullable
        private final StringMatcher bnodeDecisor;

        @Nullable
        private final StringMatcher labelDecisor;

        @Nullable
        private final StringMatcher languageDecisor;

        @Nullable
        private final StringMatcher datatypeDecisor;

        ValueMatcher(final Map<Character, StringMatcher> stringMatchers) {
            this.uriDecisor = stringMatchers.get('u');
            this.bnodeDecisor = stringMatchers.get('b');
            this.labelDecisor = stringMatchers.get('l');
            this.languageDecisor = stringMatchers.get('@');
            this.datatypeDecisor = stringMatchers.get('^');
        }

        boolean match(final Value value, final String[] groups) {
            if (value == null) {
                return this.uriDecisor == null || this.uriDecisor.match("", groups);
            }
            final String string = value.stringValue();
            if (value instanceof URI) {
                return this.uriDecisor == null || this.uriDecisor.match(string, groups);
            }
            if (value instanceof BNode) {
                return this.bnodeDecisor == null || this.bnodeDecisor.match(string, groups);
            }
            final Literal literal = (Literal) value;
            if (this.labelDecisor != null && !this.labelDecisor.match(string, groups)) {
                return false;
            }
            final String language = literal.getLanguage();
            if (language != null && this.languageDecisor != null
                    && !this.languageDecisor.match(language, groups)) {
                return false;
            }
            final URI datatype = literal.getDatatype();
            if (datatype != null && this.datatypeDecisor != null
                    && !this.datatypeDecisor.match(datatype.stringValue(), groups)) {
                return false;
            }
            return true;
        }

    }

    private static class StringMatcher {

        private final Rule[] rules;

        StringMatcher(final List<String> rules, final AtomicInteger groupCount) {

            final List<Rule> list = new ArrayList<>();
            for (final String rule : rules) {
                final boolean polarity = rule.charAt(0) == '+';
                String constant = null;
                Pattern pattern = null;
                if (rule.charAt(1) == '\'' || rule.charAt(1) == '\"' || rule.charAt(1) == '<') {
                    constant = rule.substring(2, rule.length() - 1);
                } else if (rule.charAt(1) == '|') {
                    pattern = Pattern.compile(rule.substring(2, rule.length() - 1));
                } else if (rule.contains(":")) {
                    constant = Values.parseValue(rule.substring(1)).stringValue();
                } else if (rule.charAt(1) == '[') {
                    list.add(new HashMatcherRule(Util.parseFileFilterRule(rule), polarity));
                    continue;
                } else if (rule.length() != 2 || rule.charAt(1) != '*') {
                    final String prefix = rule.substring(1);
                    final String namespace = Util.PREFIX_TO_NS_MAP.get(prefix);
                    if (namespace == null) {
                        throw new IllegalArgumentException("Unknown namespace prefix " + prefix);
                    }
                    pattern = Pattern.compile(Pattern.quote(namespace) + ".*");
                }
                int numGroups = 0;
                if (pattern != null) {
                    numGroups = pattern.matcher("").groupCount();
                }
                list.add(new ConstPatternRule(constant, pattern, polarity, groupCount.get()));
                groupCount.addAndGet(numGroups);
            }

            this.rules = list.toArray(new Rule[list.size()]);
        }

        boolean match(final String string, final String[] groups) {
            for (final Rule rule : this.rules) {
                boolean match = rule.match(string, groups);
                if (match) {
                    return rule.getPolarity();
                }
            }
            return true;
        }

        private interface Rule {

            boolean match(final String string, final String[] groups);

            boolean getPolarity();
        }

        private static class ConstPatternRule implements Rule {

            @Nullable
            final String constant;

            @Nullable
            final Pattern pattern;

            final boolean polarity;

            final int group;

            ConstPatternRule(final String constant, final Pattern pattern, final boolean polarity,
                    final int group) {
                this.constant = constant;
                this.pattern = pattern;
                this.polarity = polarity;
                this.group = group;
            }

            @Override
            public boolean match(String string, String[] groups) {
                if (constant != null) {
                    return constant.equals(string);
                } else if (pattern != null) {
                    final Matcher matcher = pattern.matcher(string);
                    final boolean match = matcher.matches();
                    if (match) {
                        final int numGroups = matcher.groupCount();
                        for (int i = 0; i < numGroups; ++i) {
                            groups[group + i] = matcher.group(i + 1);
                        }
                        return true;
                    }
                }
                return true;
            }

            @Override
            public boolean getPolarity() {
                return polarity;
            }
        }

        private static class HashMatcherRule implements Rule {

            private final Set<String> hashes;
            private final boolean polarity;

            private HashMatcherRule(Set<String> hashes, boolean polarity) {
                this.hashes = hashes;
                this.polarity = polarity;
            }

            @Override
            public boolean match(String string, String[] groups) {
                return hashes.contains(Util.murmur3Str(string));
            }

            @Override
            public boolean getPolarity() {
                return polarity;
            }
        }

    }

    private static class ValueReplacer {

        @Nullable
        private final StringReplacer uriReplacer;

        @Nullable
        private final StringReplacer bnodeReplacer;

        @Nullable
        private final StringReplacer labelReplacer;

        @Nullable
        private final StringReplacer languageReplacer;

        @Nullable
        private final StringReplacer datatypeReplacer;

        @Nullable
        private final Value uriConstant;

        @Nullable
        private final Value bnodeConstant;

        ValueReplacer(final Map<Character, StringReplacer> stringReplacers) {

            this.uriReplacer = stringReplacers.get('u');
            this.bnodeReplacer = stringReplacers.get('b');
            this.labelReplacer = stringReplacers.get('l');
            this.languageReplacer = stringReplacers.get('@');
            this.datatypeReplacer = stringReplacers.get('^');
            this.uriConstant = this.uriReplacer == null || !this.uriReplacer.isConstant() ? null
                    : Util.FACTORY.createURI(this.uriReplacer.replace("", EMPTY));
            this.bnodeConstant = this.bnodeReplacer == null || !this.bnodeReplacer.isConstant() ? null
                    : Util.FACTORY.createBNode(this.bnodeReplacer.replace("", EMPTY));
        }

        Value replace(final Value value, final String[] groups) {

            if (value == null) {
                if (this.uriReplacer == null) {
                    return value;
                }
                if (this.uriConstant != null) {
                    return this.uriConstant;
                }
                final String newString = this.uriReplacer.replace("", groups);
                return newString.equals("") ? value : Util.FACTORY.createURI(newString);
            }

            if (value instanceof URI) {
                if (this.uriReplacer == null) {
                    return value;
                }
                if (this.uriConstant != null) {
                    return this.uriConstant;
                }
                final String oldString = value.stringValue();
                final String newString = this.uriReplacer.replace(oldString, groups);
                return newString == oldString ? value : Util.FACTORY.createURI(newString);
            }

            if (value instanceof BNode) {
                if (this.bnodeReplacer == null) {
                    return value;
                }
                if (this.bnodeConstant != null) {
                    return this.bnodeConstant;
                }
                final String oldString = value.stringValue();
                final String newString = this.bnodeReplacer.replace(oldString, groups);
                return newString == oldString ? value : Util.FACTORY.createBNode(newString);
            }

            final Literal literal = (Literal) value;
            boolean changed = false;

            String label = literal.getLabel();
            if (this.labelReplacer != null) {
                final String oldLabel = label;
                label = this.labelReplacer.replace(oldLabel, groups);
                changed |= oldLabel != label;
            }

            String language = literal.getLanguage();
            if (language != null && this.languageReplacer != null) {
                final String oldLanguage = language;
                language = this.languageReplacer.replace(oldLanguage, groups);
                changed |= oldLanguage != language;
            }

            URI datatype = literal.getDatatype();
            if (datatype != null && this.datatypeReplacer != null) {
                final String oldDatatype = datatype.stringValue();
                final String newDatatype = this.datatypeReplacer.replace(oldDatatype, groups);
                if (oldDatatype == newDatatype) {
                    changed = true;
                    datatype = Util.FACTORY.createURI(newDatatype);
                }
            }

            if (!changed) {
                return value;
            } else if (language != null) {
                return Util.FACTORY.createLiteral(label, language);
            } else if (datatype != null) {
                return Util.FACTORY.createLiteral(label, datatype);
            } else {
                return Util.FACTORY.createLiteral(label);
            }
        }

    }

    private static class StringReplacer {

        private static final Pattern GROUP_PATTERN = Pattern.compile("\\\\\\d");

        private Object[] sequence;

        StringReplacer(final String replacement) {

            if (replacement.startsWith("'") || replacement.startsWith("\"")
                    || replacement.startsWith("<")) {
                this.sequence = new Object[] { replacement.substring(1, replacement.length() - 1) };

            } else if (replacement.startsWith("|")) {
                final String exp = replacement.substring(1, replacement.length() - 1);
                final Matcher matcher = GROUP_PATTERN.matcher(exp);
                final List<Object> sequence = new ArrayList<Object>();
                int index = 0;
                while (matcher.find()) {
                    if (matcher.start() - index > 0) {
                        sequence.add(exp.substring(index, matcher.start()));
                    }
                    sequence.add(Integer.valueOf(exp.substring(matcher.start() + 1, matcher.end())) - 1);
                    index = matcher.end();
                }
                if (index < exp.length()) {
                    sequence.add(exp.substring(index));
                }
                this.sequence = sequence.toArray(new Object[sequence.size()]);

            } else if (replacement.contains(":")) {
                this.sequence = new Object[] { Values.parseValue(replacement).stringValue() };

            } else {
                final String namespace = Util.PREFIX_TO_NS_MAP.get(replacement);
                if (namespace == null) {
                    throw new IllegalArgumentException("Unknown namespace prefix " + replacement);
                }
                this.sequence = new Object[] { namespace };
            }
        }

        boolean isConstant() {
            return this.sequence.length == 1 && this.sequence[0] instanceof String;
        }

        String replace(final String string, final String[] groups) {
            final StringBuilder builder = new StringBuilder();
            for (final Object element : this.sequence) {
                if (element instanceof String) {
                    builder.append(element);
                } else {
                    final String group = groups[(Integer) element];
                    builder.append(group);
                }
            }
            return builder.toString();
        }

    }

}
