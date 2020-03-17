/*
 * Copyright 2020 the original author or authors.
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

import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Collectors;

import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.OrderByField;
import org.springframework.data.relational.core.sql.SelectBuilder.SelectFromAndJoin;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.StatementBuilder;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.render.NamingStrategies;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.core.sql.render.RenderNamingStrategy;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;

/**
 * Implementation of {@link AbstractQueryCreator} that creates SQL query from a {@link PartTree}.
 *
 * @author Roman Chigvintsev
 */
public class R2dbcQueryCreator extends AbstractQueryCreator<String, Condition> {
	private final PartTree tree;
	private final ReactiveDataAccessStrategy dataAccessStrategy;
	private final RelationalEntityMetadata<?> entityMetadata;
	private final ConditionFactory conditionFactory;

	/**
	 * Creates new instance of this class with the given {@link PartTree}, {@link ReactiveDataAccessStrategy},
	 * {@link RelationalEntityMetadata} and {@link ParameterMetadataProvider}.
	 *
	 * @param tree part tree (must not be {@literal null})
	 * @param dataAccessStrategy data access strategy (must not be {@literal null})
	 * @param entityMetadata relational entity metadata (must not be {@literal null})
	 * @param parameterMetadataProvider parameter metadata provider (must not be {@literal null})
	 */
	public R2dbcQueryCreator(PartTree tree, ReactiveDataAccessStrategy dataAccessStrategy,
			RelationalEntityMetadata<?> entityMetadata, ParameterMetadataProvider parameterMetadataProvider) {
		super(tree);
		this.tree = tree;

		Assert.notNull(dataAccessStrategy, "Data access strategy must not be null");
		Assert.notNull(entityMetadata, "Relational entity metadata must not be null");
		Assert.notNull(parameterMetadataProvider, "Parameter metadata provider must not be null");

		this.dataAccessStrategy = dataAccessStrategy;
		this.entityMetadata = entityMetadata;

		RenderContext renderContext = dataAccessStrategy.getStatementMapper().getRenderContext();
		RenderNamingStrategy namingStrategy = renderContext == null ? NamingStrategies.asIs()
				: renderContext.getNamingStrategy();
		this.conditionFactory = new ConditionFactory(dataAccessStrategy, namingStrategy, parameterMetadataProvider);
	}

	/**
	 * Creates {@link Condition} for the given method name part.
	 *
	 * @param part method name part (must not be {@literal null})
	 * @param iterator iterator over query parameter values
	 * @return new instance of {@link Condition}
	 */
	@Override
	protected Condition create(Part part, Iterator<Object> iterator) {
		return conditionFactory.createCondition(part);
	}

	/**
	 * Combines the given {@link Condition} with the new one created for the given method name part using {@code AND}.
	 *
	 * @param part method name part (must not be {@literal null})
	 * @param base condition to be combined (must not be {@literal null})
	 * @param iterator iterator over query parameter values
	 * @return condition combination
	 */
	@Override
	protected Condition and(Part part, Condition base, Iterator<Object> iterator) {
		return base.and(conditionFactory.createCondition(part));
	}

	/**
	 * Combines two {@link Condition}s using {@code OR}.
	 *
	 * @param base condition to be combined (must not be {@literal null})
	 * @param condition another condition to be combined (must not be {@literal null})
	 * @return condition combination
	 */
	@Override
	protected Condition or(Condition base, Condition condition) {
		return base.or(condition);
	}

	/**
	 * Creates SQL query applying the given {@link Condition} and {@link Sort} definition.
	 *
	 * @param condition condition to be applied to query
	 * @param sort sort option to be applied to query (must not be {@literal null})
	 * @return SQL query
	 */
	@Override
	protected String complete(Condition condition, Sort sort) {
		Table fromTable = Table.create(dataAccessStrategy.toSql(entityMetadata.getTableName()));
		Collection<? extends Expression> selectExpressions = getSelectionExpressions(fromTable);
		SelectFromAndJoin selectBuilder = StatementBuilder.select(selectExpressions).from(fromTable);

		if (tree.isExistsProjection()) {
			selectBuilder.limit(1);
		} else if (tree.isLimiting()) {
			selectBuilder.limit(tree.getMaxResults());
		}

		if (condition != null) {
			selectBuilder.where(condition);
		}

		if (sort.isSorted()) {
			selectBuilder.orderBy(getOrderBySegments(sort, fromTable));
		}

		RenderContext renderContext = dataAccessStrategy.getStatementMapper().getRenderContext();
		SqlRenderer sqlRenderer = renderContext == null ? SqlRenderer.create() : SqlRenderer.create(renderContext);
		return sqlRenderer.render(selectBuilder.build());
	}

	private Collection<? extends Expression> getSelectionExpressions(Table fromTable) {
		if (tree.isExistsProjection()) {
			return fromTable.columns(toSql(dataAccessStrategy.getIdentifierColumns(entityMetadata.getJavaType())));
		}
		return fromTable.columns(toSql(dataAccessStrategy.getAllColumns(entityMetadata.getJavaType())));
	}

	private Collection<? extends OrderByField> getOrderBySegments(Sort sort, Table fromTable) {
		RelationalPersistentEntity<?> tableEntity = entityMetadata.getTableEntity();
		return sort.get().map(order -> {
			RelationalPersistentProperty property = tableEntity.getRequiredPersistentProperty(order.getProperty());
			Column column = fromTable.column(dataAccessStrategy.toSql(property.getColumnName()));
			// TODO: org.springframework.data.relational.core.sql.render.OrderByClauseVisitor from
			// spring-data-relational does not prepend column name with table name. It makes sense to render
			// column names uniformly.
			OrderByField orderByField = OrderByField.from(column);
			if (order.isAscending()) {
				orderByField = orderByField.asc();
			} else {
				orderByField = orderByField.desc();
			}
			return orderByField;
		}).collect(Collectors.toList());
	}

	private Collection<String> toSql(Collection<? extends SqlIdentifier> identifiers) {
		Assert.notNull(identifiers, "SQL identifiers must not be null");
		return identifiers.stream().map(dataAccessStrategy::toSql).collect(Collectors.toList());
	}
}
