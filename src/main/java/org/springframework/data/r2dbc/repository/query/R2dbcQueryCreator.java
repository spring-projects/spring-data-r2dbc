/*
 * Copyright 2018-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.r2dbc.repository.query;

import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.SelectBuilder.SelectFromAndJoin;
import org.springframework.data.relational.core.sql.StatementBuilder;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.Iterator;

/**
 * Implementation of {@link AbstractQueryCreator} that creates {@link BindableQuery} from a {@link PartTree}.
 *
 * @author Roman Chigvintsev
 */
public class R2dbcQueryCreator extends AbstractQueryCreator<String, Condition> {
    private final PartTree tree;
    private final ReactiveDataAccessStrategy dataAccessStrategy;
    private final RelationalEntityMetadata<?> entityMetadata;
    private final ConditionFactory conditionFactory;

    /**
     * Creates new instance of this class with the given {@link PartTree},  {@link ReactiveDataAccessStrategy},
     * {@link RelationalEntityMetadata} and {@link ParameterMetadataProvider}.
     *
     * @param tree                      part tree (must not be {@literal null})
     * @param dataAccessStrategy        data access strategy (must not be {@literal null})
     * @param entityMetadata            relational entity metadata (must not be {@literal null})
     * @param parameterMetadataProvider parameter metadata provider (must not be {@literal null})
     */
    public R2dbcQueryCreator(PartTree tree,
                             ReactiveDataAccessStrategy dataAccessStrategy,
                             RelationalEntityMetadata<?> entityMetadata,
                             ParameterMetadataProvider parameterMetadataProvider) {
        super(tree);
        this.tree = tree;

        Assert.notNull(dataAccessStrategy, "Data access strategy must not be null");
        Assert.notNull(entityMetadata, "Relational entity metadata must not be null");
        Assert.notNull(parameterMetadataProvider, "Parameter metadata provider must not be null");

        this.dataAccessStrategy = dataAccessStrategy;
        this.entityMetadata = entityMetadata;

        R2dbcConverter converter = dataAccessStrategy.getConverter();
        this.conditionFactory = new ConditionFactory(converter.getMappingContext(), parameterMetadataProvider);
    }

    @Override
    protected Condition create(Part part, Iterator<Object> iterator) {
        return conditionFactory.createCondition(part);
    }

    @Override
    protected Condition and(Part part, Condition base, Iterator<Object> iterator) {
        return base.and(conditionFactory.createCondition(part));
    }

    @Override
    protected Condition or(Condition base, Condition condition) {
        return base.or(condition);
    }

    /**
     * Creates {@link BindableQuery} applying the given {@link Condition} and {@link Sort} definition.
     *
     * @param condition condition to be applied to query
     * @param sort      sort option to be applied to query (must not be {@literal null})
     * @return new instance of {@link BindableQuery}
     */
    @Override
    protected String complete(Condition condition, Sort sort) {
        Table fromTable = Table.create(entityMetadata.getTableName());
        Collection<? extends Expression> selectExpressions = getSelectionExpressions(fromTable);
        SelectFromAndJoin selectBuilder = StatementBuilder.select(selectExpressions).from(fromTable);

        if (tree.isExistsProjection()) {
            selectBuilder.limit(1);
        }

        if (condition != null) {
            selectBuilder.where(condition);
        }

        RenderContext renderContext = dataAccessStrategy.getStatementMapper().getRenderContext();
        SqlRenderer sqlRenderer = renderContext == null ? SqlRenderer.create() : SqlRenderer.create(renderContext);
        return sqlRenderer.render(selectBuilder.build());
    }

    private Collection<? extends Expression> getSelectionExpressions(Table fromTable) {
        if (tree.isExistsProjection()) {
            return fromTable.columns(dataAccessStrategy.getIdentifierColumns(entityMetadata.getJavaType()));
        }
        return fromTable.columns(dataAccessStrategy.getAllColumns(entityMetadata.getJavaType()));
    }
}
