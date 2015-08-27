package eu.fbk.rdfpro.rules.util;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import com.google.common.collect.Lists;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;

import eu.fbk.rdfpro.util.Statements;

public enum StatementComponent implements Function<Statement, Value>, Comparator<Statement> {

    SUBJECT('s', (byte) 0) {

        @Override
        public Resource apply(final Statement statement) {
            return statement.getSubject();
        }

        @Override
        public Statement replace(final Statement statement, final Value value,
                final ValueFactory factory) {
            if (Objects.equals(statement.getSubject(), value)) {
                return statement;
            } else if (statement.getContext() == null) {
                return factory.createStatement((Resource) value, statement.getPredicate(),
                        statement.getObject());
            } else {
                return factory.createStatement((Resource) value, statement.getPredicate(),
                        statement.getObject(), statement.getContext());
            }
        }

    },

    PREDICATE('p', (byte) 1) {

        @Override
        public URI apply(final Statement statement) {
            return statement.getPredicate();
        }

        @Override
        public Statement replace(final Statement statement, final Value value,
                final ValueFactory factory) {
            if (Objects.equals(statement.getPredicate(), value)) {
                return statement;
            } else if (statement.getContext() == null) {
                return factory.createStatement(statement.getSubject(), (URI) value,
                        statement.getObject());
            } else {
                return factory.createStatement(statement.getSubject(), (URI) value,
                        statement.getObject(), statement.getContext());
            }
        }

    },

    OBJECT('o', (byte) 2) {

        @Override
        public Value apply(final Statement statement) {
            return statement.getObject();
        }

        @Override
        public Statement replace(final Statement statement, final Value value,
                final ValueFactory factory) {
            if (Objects.equals(statement.getContext(), value)) {
                return statement;
            } else if (value == null) {
                return factory.createStatement(statement.getSubject(), statement.getPredicate(),
                        statement.getObject());
            } else {
                return factory.createStatement(statement.getSubject(), statement.getPredicate(),
                        statement.getObject(), (Resource) value);
            }
        }

    },

    CONTEXT('c', (byte) 3) {

        @Override
        public Resource apply(final Statement statement) {
            return statement.getContext();
        }

        @Override
        public Statement replace(final Statement statement, final Value value,
                final ValueFactory factory) {
            if (Objects.equals(statement.getObject(), value)) {
                return statement;
            } else if (statement.getContext() == null) {
                return factory.createStatement(statement.getSubject(), statement.getPredicate(),
                        value);
            } else {
                return factory.createStatement(statement.getSubject(), statement.getPredicate(),
                        value, statement.getContext());
            }
        }

    };

    private char letter;

    private byte index;

    private StatementComponent(final char letter, final byte index) {
        this.letter = letter;
        this.index = index;
    }

    public final char getLetter() {
        return this.letter;
    }

    public byte getIndex() {
        return this.index;
    }

    @Override
    public abstract Value apply(final Statement statement);

    @Override
    public final int compare(final Statement first, final Statement second) {
        if (first == null && second == null) {
            return 0;
        } else if (first != null && second == null) {
            return 1;
        } else if (first == null && second != null) {
            return -1;
        } else {
            return Statements.valueComparator().compare(apply(first), apply(second));
        }
    }

    public final Statement replace(final Statement statement, final Value value) {
        return replace(statement, value, Statements.VALUE_FACTORY);
    }

    public abstract Statement replace(Statement statement, final Value value, ValueFactory factory);

    public static StatementComponent forLetter(char letter) {
        letter = Character.toLowerCase(letter);
        for (final StatementComponent component : values()) {
            if (component.getLetter() == letter) {
                return component;
            }
        }
        throw new IllegalArgumentException("Unknown statement component '" + letter + "'");
    }

    public static List<StatementComponent> forLetters(final CharSequence letters) {
        return forLetters(letters, false);
    }

    public static List<StatementComponent> forLetters(final CharSequence letters,
            final boolean acceptDuplicates) {
        final List<StatementComponent> result = Lists.newArrayList();
        for (int i = 0; i < letters.length(); ++i) {
            final StatementComponent component = forLetter(letters.charAt(i));
            if (!acceptDuplicates && result.contains(component)) {
                throw new IllegalArgumentException("Duplicate statement component " + component);
            }
            result.add(component);
        }
        return result;
    }

}
