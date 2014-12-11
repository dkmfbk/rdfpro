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

import javax.annotation.Nullable;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

/**
 * Reduce function in a MapReduce job.
 * <p>
 * A {@code Reducer} object is used in a MapReduce job (see
 * {@link RDFProcessors#mapReduce(Mapper, Reducer, boolean)}) to process a partition of statements
 * associated to a certain {@code Value} key produced by a {@link Mapper} in a previous map phase
 * (e.g., all the statements having a specific subject).
 * </p>
 * <p>
 * Implementations of this interface should be thread-safe, as multiple reduce jobs can be fired
 * in parallel with method {@code reduce()} being invoked concurrently by different threads on
 * different statement partitions.
 * </p>
 */
@FunctionalInterface
public interface Reducer {

    /**
     * Processes the statement partition associated to a certain key, emitting output statements
     * to the supplied {@code RDFHandler}.
     *
     * @param key
     *            the partition key, possibly null
     * @param statements
     *            a modifiable array with the statements belonging to the partition, not null
     * @param handler
     *            the {@code RDFHandler} where to emit output statements, not null
     * @throws RDFHandlerException
     *             on error
     */
    void reduce(@Nullable Value key, Statement[] statements, RDFHandler handler)
            throws RDFHandlerException;

}