package org.springframework.data.r2dbc.repository.query;

import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.SelectBuilder.SelectFromAndJoin;
import org.springframework.data.relational.core.sql.StatementBuilder;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Condition;

/**
 * Implementation of {@link AbstractQueryCreator} that creates {@link BindableQuery} from a {@link PartTree}.
 *
 * @author Roman Chigvintsev
 */
class R2dbcQueryCreator extends AbstractQueryCreator<BindableQuery, Condition> {
    private final PartTree tree;
    private final RelationalEntityMetadata<?> entityMetadata;
    private final ReactiveDataAccessStrategy dataAccessStrategy;

    /**
     * Creates new instance of this class with the given {@link PartTree}, {@link RelationalEntityMetadata} and
     * {@link ReactiveDataAccessStrategy}.
     *
     * @param tree part tree (must not be {@literal null})
     * @param entityMetadata relational entity metadata (must not be {@literal null})
     * @param dataAccessStrategy data access strategy (must not be {@literal null})
     */
    public R2dbcQueryCreator(PartTree tree,
                             RelationalEntityMetadata<?> entityMetadata,
                             ReactiveDataAccessStrategy dataAccessStrategy) {
        super(tree);
        this.tree = tree;

        Assert.notNull(entityMetadata, "Relational entity metadata must not be null");
        Assert.notNull(dataAccessStrategy, "Data access strategy must not be null");

        this.entityMetadata = entityMetadata;
        this.dataAccessStrategy = dataAccessStrategy;
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

    /**
     * Creates {@link BindableQuery} applying the given {@link Condition} and {@link Sort} definition.
     *
     * @param condition condition to be applied to query
     * @param sort sort option to be applied to query (must not be {@literal null})
     * @return new instance of {@link BindableQuery}
     */
    @Override
    protected BindableQuery complete(Condition condition, Sort sort) {
        Table fromTable = Table.create(entityMetadata.getTableName());
        List<Column> columns = fromTable.columns(dataAccessStrategy.getAllColumns(entityMetadata.getJavaType()));

        SelectFromAndJoin selectBuilder = StatementBuilder.select(columns).from(fromTable);
        if (tree.isExistsProjection()) {
            selectBuilder.limit(1);
        }

        SqlRenderer sqlRenderer = SqlRenderer.create();
        String sql = sqlRenderer.render(selectBuilder.build());

        return new BindableQuery() {
            @Override
            public <T extends DatabaseClient.BindSpec<T>> T bind(T bindSpec) {
                return bindSpec;
            }

            @Override
            public String get() {
                return sql;
            }
        };
    }
}
