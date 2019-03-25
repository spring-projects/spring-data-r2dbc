/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.r2dbc.function;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.convert.CustomConversions.StoreConversions;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.r2dbc.dialect.ArrayColumns;
import org.springframework.data.r2dbc.dialect.BindMarkers;
import org.springframework.data.r2dbc.dialect.BindMarkersFactory;
import org.springframework.data.r2dbc.dialect.Dialect;
import org.springframework.data.r2dbc.domain.OutboundRow;
import org.springframework.data.r2dbc.domain.SettableValue;
import org.springframework.data.r2dbc.function.convert.EntityRowMapper;
import org.springframework.data.r2dbc.function.convert.MappingR2dbcConverter;
import org.springframework.data.r2dbc.function.convert.R2dbcConverter;
import org.springframework.data.r2dbc.function.convert.R2dbcCustomConversions;
import org.springframework.data.r2dbc.function.query.BoundCondition;
import org.springframework.data.r2dbc.function.query.Criteria;
import org.springframework.data.r2dbc.function.query.CriteriaMapper;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.render.NamingStrategies;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.core.sql.render.RenderNamingStrategy;
import org.springframework.data.relational.core.sql.render.SelectRenderContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Default {@link ReactiveDataAccessStrategy} implementation.
 *
 * @author Mark Paluch
 */
public class DefaultReactiveDataAccessStrategy implements ReactiveDataAccessStrategy {

	private final Dialect dialect;
	private final R2dbcConverter converter;
	private final CriteriaMapper criteriaMapper;
	private final MappingContext<RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext;
	private final StatementFactory statements;

	/**
	 * Creates a new {@link DefaultReactiveDataAccessStrategy} given {@link Dialect}.
	 *
	 * @param dialect the {@link Dialect} to use.
	 */
	public DefaultReactiveDataAccessStrategy(Dialect dialect) {
		this(dialect, createConverter(dialect));
	}

	private static R2dbcConverter createConverter(Dialect dialect) {

		Assert.notNull(dialect, "Dialect must not be null");

		R2dbcCustomConversions customConversions = new R2dbcCustomConversions(
				StoreConversions.of(dialect.getSimpleTypeHolder()), Collections.emptyList());

		RelationalMappingContext context = new RelationalMappingContext();
		context.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());

		return new MappingR2dbcConverter(context, customConversions);
	}

	/**
	 * Creates a new {@link DefaultReactiveDataAccessStrategy} given {@link Dialect} and {@link R2dbcConverter}.
	 *
	 * @param dialect the {@link Dialect} to use.
	 * @param converter must not be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	public DefaultReactiveDataAccessStrategy(Dialect dialect, R2dbcConverter converter) {

		Assert.notNull(dialect, "Dialect must not be null");
		Assert.notNull(converter, "RelationalConverter must not be null");

		this.converter = converter;
		this.criteriaMapper = new CriteriaMapper(converter);
		this.mappingContext = (MappingContext<RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty>) this.converter
				.getMappingContext();
		this.dialect = dialect;

		RenderContext renderContext = new RenderContext() {
			@Override
			public RenderNamingStrategy getNamingStrategy() {
				return NamingStrategies.asIs();
			}

			@Override
			public SelectRenderContext getSelect() {
				return new SelectRenderContext() {
					@Override
					public Function<Select, ? extends CharSequence> afterSelectList() {
						return it -> "";
					}

					@Override
					public Function<Select, ? extends CharSequence> afterOrderBy(boolean hasOrderBy) {
						return it -> "";
					}
				};
			}
		};

		this.statements = new DefaultStatementFactory(this.dialect, renderContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getAllFields(java.lang.Class)
	 */
	@Override
	public List<String> getAllColumns(Class<?> typeToRead) {

		RelationalPersistentEntity<?> persistentEntity = getPersistentEntity(typeToRead);

		if (persistentEntity == null) {
			return Collections.singletonList("*");
		}

		List<String> columnNames = new ArrayList<>();
		for (RelationalPersistentProperty property : persistentEntity) {
			columnNames.add(property.getColumnName());
		}

		return columnNames;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getOutboundRow(java.lang.Object)
	 */
	public OutboundRow getOutboundRow(Object object) {

		Assert.notNull(object, "Entity object must not be null!");

		OutboundRow row = new OutboundRow();

		converter.write(object, row);

		RelationalPersistentEntity<?> entity = getRequiredPersistentEntity(ClassUtils.getUserClass(object));

		for (RelationalPersistentProperty property : entity) {

			SettableValue value = row.get(property.getColumnName());
			if (shouldConvertArrayValue(property, value)) {

				SettableValue writeValue = getArrayValue(value, property);
				row.put(property.getColumnName(), writeValue);
			}
		}

		return row;
	}

	private boolean shouldConvertArrayValue(RelationalPersistentProperty property, SettableValue value) {
		return value != null && value.hasValue() && property.isCollectionLike();
	}

	private SettableValue getArrayValue(SettableValue value, RelationalPersistentProperty property) {

		ArrayColumns arrayColumns = dialect.getArraySupport();

		if (!arrayColumns.isSupported()) {

			throw new InvalidDataAccessResourceUsageException(
					"Dialect " + dialect.getClass().getName() + " does not support array columns");
		}

		return SettableValue.fromOrEmpty(converter.getArrayValue(arrayColumns, property, value.getValue()),
				property.getActualType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getMappedSort(java.lang.Class, org.springframework.data.domain.Sort)
	 */
	@Override
	public Sort getMappedSort(Sort sort, Class<?> typeToRead) {

		RelationalPersistentEntity<?> entity = getPersistentEntity(typeToRead);
		if (entity == null) {
			return sort;
		}

		List<Order> mappedOrder = new ArrayList<>();

		for (Order order : sort) {

			RelationalPersistentProperty persistentProperty = entity.getPersistentProperty(order.getProperty());
			if (persistentProperty == null) {
				mappedOrder.add(order);
			} else {
				mappedOrder
						.add(Order.by(persistentProperty.getColumnName()).with(order.getNullHandling()).with(order.getDirection()));
			}
		}

		return Sort.by(mappedOrder);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getMappedCriteria(org.springframework.data.r2dbc.function.query.Criteria, org.springframework.data.relational.core.sql.Table)
	 */
	@Override
	public BoundCondition getMappedCriteria(Criteria criteria, Table table) {
		return getMappedCriteria(criteria, table, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getMappedCriteria(org.springframework.data.r2dbc.function.query.Criteria, org.springframework.data.relational.core.sql.Table, java.lang.Class)
	 */
	@Override
	public BoundCondition getMappedCriteria(Criteria criteria, Table table, @Nullable Class<?> typeToRead) {

		BindMarkers bindMarkers = this.dialect.getBindMarkersFactory().create();

		RelationalPersistentEntity<?> entity = typeToRead != null ? mappingContext.getRequiredPersistentEntity(typeToRead)
				: null;

		return criteriaMapper.getMappedObject(bindMarkers, criteria, table, entity);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getRowMapper(java.lang.Class)
	 */
	@Override
	public <T> BiFunction<Row, RowMetadata, T> getRowMapper(Class<T> typeToRead) {
		return new EntityRowMapper<>(typeToRead, converter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getTableName(java.lang.Class)
	 */
	@Override
	public String getTableName(Class<?> type) {
		return getRequiredPersistentEntity(type).getTableName();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getStatements()
	 */
	@Override
	public StatementFactory getStatements() {
		return this.statements;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getBindMarkersFactory()
	 */
	@Override
	public BindMarkersFactory getBindMarkersFactory() {
		return dialect.getBindMarkersFactory();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy#getConverter()
	 */
	public R2dbcConverter getConverter() {
		return converter;
	}

	public MappingContext<RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> getMappingContext() {
		return mappingContext;
	}

	private RelationalPersistentEntity<?> getRequiredPersistentEntity(Class<?> typeToRead) {
		return mappingContext.getRequiredPersistentEntity(typeToRead);
	}

	@Nullable
	private RelationalPersistentEntity<?> getPersistentEntity(Class<?> typeToRead) {
		return mappingContext.getPersistentEntity(typeToRead);
	}
}
