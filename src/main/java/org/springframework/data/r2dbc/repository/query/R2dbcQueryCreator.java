package org.springframework.data.r2dbc.repository.query;

import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;

import java.util.Iterator;
import java.util.concurrent.locks.Condition;

/**
 * Implementation of {@link AbstractQueryCreator} that creates {@link BindableQuery} from a {@link PartTree}.
 *
 * @author Roman Chigvintsev
 */
class R2dbcQueryCreator extends AbstractQueryCreator<BindableQuery, Condition> {
    /**
     * Creates new instance of this class with the given {@link PartTree}.
     *
     * @param tree part tree (must not be {@literal null})
     */
    public R2dbcQueryCreator(PartTree tree) {
        super(tree);
    }

    @Override
    protected Condition create(Part part, Iterator<Object> iterator) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Condition and(Part part, Condition condition, Iterator<Object> iterator) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Condition or(Condition condition, Condition s1) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected BindableQuery complete(Condition condition, Sort sort) {
        throw new UnsupportedOperationException();
    }
}
