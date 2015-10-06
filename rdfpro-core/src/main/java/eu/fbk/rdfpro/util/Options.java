/*
 * RDFpro - An extensible tool for building stream-oriented RDF processing libraries.
 * 
 * Written in 2014 by Francesco Corcoglioniti with support by Marco Amadori, Michele Mostarda,
 * Alessio Palmero Aprosio and Marco Rospocher. Contact info on http://rdfpro.fbk.eu/
 * 
 * To the extent possible under law, the authors have dedicated all copyright and related and
 * neighboring rights to this software to the public domain worldwide. This software is
 * distributed without any warranty.
 * 
 * You should have received a copy of the CC0 Public Domain Dedication along with this software.
 * If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package eu.fbk.rdfpro.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

public final class Options {

    private final List<String> positionalArgs;

    private final List<String> options;

    private final Map<String, List<String>> optionArgs;

    public static Options parse(final String spec, final String... args) {

        final String[][] argsToOptions = new String[args.length][];
        final int[] argsToMinCard = new int[args.length];
        final int[] argsToMaxCard = new int[args.length];
        final List<String> mandatoryOptions = new ArrayList<String>();
        int minPositionalCard = 0;
        int maxPositionalCard = 0;

        for (final String tokenUntrimmed : spec.split("\\|")) {
            final String token = tokenUntrimmed.trim();
            final int len = token.length();
            if (len == 0) {
                continue;
            }
            final char last = token.charAt(len - 1);
            final int minCard = last == '!' || last == '+' ? 1 : 0;
            final int maxCard = last == '?' || last == '!' ? 1
                    : last == '+' || last == '*' ? Integer.MAX_VALUE : 0;
            final boolean mandatory = minCard >= 1 && len >= 2 && token.charAt(len - 2) == last;
            final String tokenOptions = token.substring(0, maxCard == 0 ? len
                    : mandatory ? len - 2 : len - 1);
            if (tokenOptions.length() == 0) {
                minPositionalCard = minCard;
                maxPositionalCard = maxCard;
            } else {
                final String[] options = tokenOptions.split(",");
                for (int i = 0; i < options.length; ++i) {
                    options[i] = options[i].trim();
                    if (mandatory) {
                        mandatoryOptions.add(options[i]);
                    }
                }
                for (int i = 0; i < options.length; ++i) {
                    final String optionExt = (options[i].length() == 1 ? "-" : "--") + options[i];
                    for (int j = 0; j < args.length; ++j) {
                        if (args[j].equals(optionExt)) {
                            argsToOptions[j] = options;
                            argsToMinCard[j] = minCard;
                            argsToMaxCard[j] = maxCard;
                        }
                    }
                }
            }
        }

        final List<String> positionalArgs = new ArrayList<String>();
        final List<String> optionList = new ArrayList<String>();
        final Map<String, List<String>> optionArgs = new HashMap<String, List<String>>();
        int j = 0;
        while (j < args.length) {
            final String[] options = argsToOptions[j];
            if (options != null) {
                final String optionWithDashes = args[j];
                final int minCard = argsToMinCard[j];
                final int maxCard = argsToMaxCard[j];
                ++j;
                List<String> values = optionArgs.get(options[0]);
                if (values == null) {
                    values = new ArrayList<String>();
                    optionList.add(options[0]);
                    for (int i = 0; i < options.length; ++i) {
                        optionArgs.put(options[i], values);
                    }
                }
                for (int k = 0; k < maxCard; ++k) {
                    if (j < args.length && argsToOptions[j] == null) {
                        values.add(args[j++]);
                    } else if (k < minCard) {
                        throw new IllegalArgumentException("Expected at least " + minCard
                                + " arguments for option '" + optionWithDashes + "'");
                    } else {
                        break;
                    }
                }
            } else if (args[j].startsWith("-") && !args[j].contains(" ")) {
                throw new IllegalArgumentException("Unrecognized option '" + args[j] + "'");
            } else {
                positionalArgs.add(args[j++]);
            }
        }

        if (positionalArgs.size() < minPositionalCard) {
            throw new IllegalArgumentException("Expected at least " + minPositionalCard
                    + " positional arguments");
        } else if (positionalArgs.size() > maxPositionalCard) {
            throw new IllegalArgumentException("Expected at most " + maxPositionalCard
                    + " positional arguments");
        }

        for (final String option : mandatoryOptions) {
            if (!optionArgs.containsKey(option)) {
                throw new IllegalArgumentException("Missing mandatory option '" + option + "'");
            }
        }

        return new Options(positionalArgs, optionList, optionArgs);
    }

    private Options(final List<String> args, final List<String> options,
            final Map<String, List<String>> optionValues) {

        this.positionalArgs = args;
        this.options = new ArrayList<String>(options);
        this.optionArgs = optionValues;

        Collections.sort(this.options);
    }

    public <T> List<T> getPositionalArgs(final Class<T> type) {
        return convert(this.positionalArgs, type);
    }

    public <T> T getPositionalArg(final int index, final Class<T> type) {
        return convert(this.positionalArgs.get(index), type);
    }

    public <T> T getPositionalArg(final int index, final Class<T> type, final T defaultValue) {
        try {
            return convert(this.positionalArgs.get(index), type);
        } catch (final Throwable ex) {
            return defaultValue;
        }
    }

    public int getPositionalArgCount() {
        return this.positionalArgs.size();
    }

    public List<String> getOptions() {
        return this.options;
    }

    public boolean hasOption(final String optionName) {
        return this.optionArgs.containsKey(optionName);
    }

    public <T> List<T> getOptionArgs(final String optionName, final Class<T> type) {
        final List<String> strings = this.optionArgs.get(optionName);
        if (strings != null) {
            return convert(strings, type);
        }
        return Collections.emptyList();
    }

    @Nullable
    public <T> T getOptionArg(final String optionName, final Class<T> type) {
        final List<String> strings = this.optionArgs.get(optionName);
        if (strings == null || strings.isEmpty()) {
            return null;
        }
        if (strings.size() > 1) {
            throw new IllegalArgumentException("Multiple args for option '" + optionName + "': "
                    + String.join(", ", strings));
        }
        try {
            return convert(strings.get(0), type);
        } catch (final Throwable ex) {
            throw new IllegalArgumentException("'" + strings.get(0) + "' is not a valid "
                    + type.getSimpleName(), ex);
        }
    }

    @Nullable
    public <T> T getOptionArg(final String optionName, final Class<T> type,
            @Nullable final T defaultValue) {
        final List<String> strings = this.optionArgs.get(optionName);
        if (strings == null || strings.isEmpty() || strings.size() > 1) {
            return defaultValue;
        }
        try {
            return convert(strings.get(0), type);
        } catch (final Throwable ex) {
            return defaultValue;
        }
    }

    public int getOptionCount() {
        return this.options.size();
    }

    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof Options)) {
            return false;
        }
        final Options other = (Options) object;
        return this.positionalArgs.equals(other.positionalArgs)
                && this.optionArgs.equals(other.optionArgs);
    }

    @Override
    public int hashCode() {
        return this.positionalArgs.hashCode() * 37 + this.optionArgs.hashCode();
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        for (final String option : this.options) {
            builder.append(builder.length() != 0 ? " " : "").append(option);
            final List<String> args = this.optionArgs.get(option);
            if (args != null) {
                for (final String arg : args) {
                    builder.append(' ').append(arg);
                }
            }
        }
        for (final String arg : this.positionalArgs) {
            builder.append(builder.length() != 0 ? " " : "").append(arg);
        }
        return builder.toString();
    }

    private static <T> T convert(final String string, final Class<T> type) {
        try {
            return Statements.convert(string, type);
        } catch (final Throwable ex) {
            throw new IllegalArgumentException("'" + string + "' is not a valid "
                    + type.getSimpleName(), ex);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> convert(final List<String> strings, final Class<T> type) {
        if (type == String.class) {
            return (List<T>) strings;
        }
        final List<T> list = new ArrayList<T>();
        for (final String string : strings) {
            list.add(convert(string, type));
        }
        return Collections.unmodifiableList(list);
    }

}
