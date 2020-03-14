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

import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.*;
import org.springframework.data.relational.core.sql.render.RenderNamingStrategy;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

/**
 * Simple factory to contain logic to create {@link Condition}s from {@link Part}s.
 *
 * @author Roman Chigvintsev
 */
class ConditionFactory {
	private final ReactiveDataAccessStrategy dataAccessStrategy;
	private final RenderNamingStrategy namingStrategy;
	private final ParameterMetadataProvider parameterMetadataProvider;

	/**
	 * Creates new instance of this class with the given {@link ReactiveDataAccessStrategy}, {@link RenderNamingStrategy}
	 * and {@link ParameterMetadataProvider}.
	 *
	 * @param dataAccessStrategy data access strategy (must not be {@literal null})
	 * @param namingStrategy naming strategy for SQL rendering (must not be {@literal null})
	 * @param parameterMetadataProvider parameter metadata provider (must not be {@literal null})
	 */
	ConditionFactory(ReactiveDataAccessStrategy dataAccessStrategy, RenderNamingStrategy namingStrategy,
			ParameterMetadataProvider parameterMetadataProvider) {
		Assert.notNull(dataAccessStrategy, "Reactive data access strategy must not be null!");
		Assert.notNull(namingStrategy, "Render naming strategy must not be null!");
		Assert.notNull(parameterMetadataProvider, "Parameter metadata provider must not be null!");

		this.dataAccessStrategy = dataAccessStrategy;
		this.namingStrategy = namingStrategy;
		this.parameterMetadataProvider = parameterMetadataProvider;
	}

	/**
	 * Creates condition for the given {@link Part}.
	 *
	 * @param part method name part (must not be {@literal null})
	 * @return {@link Condition} instance
	 * @throws IllegalArgumentException if part type is not supported
	 */
	public Condition createCondition(Part part) {
		Part.Type type = part.getType();
		switch (type) {
			case BETWEEN: {
				Expression pathExpression = createPropertyPathExpression(part.getProperty());
				BindMarker firstBindMarker = createBindMarker(parameterMetadataProvider.next(part));
				BindMarker secondBindMarker = createBindMarker(parameterMetadataProvider.next(part));

				// TODO: why do not we have BETWEEN condition?
				return Conditions.isGreaterOrEqualTo(pathExpression, firstBindMarker)
						.and(Conditions.isLessOrEqualTo(pathExpression, secondBindMarker));
			}
			case AFTER:
			case GREATER_THAN: {
				Expression pathExpression = createPropertyPathExpression(part.getProperty());
				BindMarker bindMarker = createBindMarker(parameterMetadataProvider.next(part));
				return Conditions.isGreater(pathExpression, bindMarker);
			}
			case GREATER_THAN_EQUAL: {
				Expression pathExpression = createPropertyPathExpression(part.getProperty());
				BindMarker bindMarker = createBindMarker(parameterMetadataProvider.next(part));
				return Conditions.isGreaterOrEqualTo(pathExpression, bindMarker);
			}
			case BEFORE:
			case LESS_THAN: {
				Expression pathExpression = createPropertyPathExpression(part.getProperty());
				BindMarker bindMarker = createBindMarker(parameterMetadataProvider.next(part));
				return Conditions.isLess(pathExpression, bindMarker);
			}
			case LESS_THAN_EQUAL: {
				Expression pathExpression = createPropertyPathExpression(part.getProperty());
				BindMarker bindMarker = createBindMarker(parameterMetadataProvider.next(part));
				return Conditions.isLessOrEqualTo(pathExpression, bindMarker);
			}
			case IS_NULL:
			case IS_NOT_NULL: {
				IsNull isNullCondition = Conditions.isNull(createPropertyPathExpression(part.getProperty()));
				return part.getType() == Part.Type.IS_NULL ? isNullCondition : isNullCondition.not();
			}
			case IN:
			case NOT_IN: {
				Expression pathExpression = upperIfIgnoreCase(part, createPropertyPathExpression(part.getProperty()));
				BindMarker bindMarker = createBindMarker(parameterMetadataProvider.next(part));
				In inCondition = Conditions.in(pathExpression, bindMarker);
				return part.getType() == Part.Type.IN ? inCondition : inCondition.not();
			}
			case STARTING_WITH:
			case ENDING_WITH:
			case CONTAINING:
			case NOT_CONTAINING:
			case LIKE:
			case NOT_LIKE: {
				Expression pathExpression = createPropertyPathExpression(part.getProperty());
				ParameterMetadata parameterMetadata = parameterMetadataProvider.next(part);
				BindMarker bindMarker = createBindMarker(parameterMetadata);
				Expression lhs = upperIfIgnoreCase(part, pathExpression);
				Expression rhs = upperIfIgnoreCase(part, bindMarker, parameterMetadata.getType());
				return part.getType() == Part.Type.NOT_LIKE || part.getType() == Part.Type.NOT_CONTAINING
						? NotLike.create(lhs, rhs)
						: Conditions.like(lhs, rhs);
			}
			case TRUE:
			case FALSE: {
				Expression pathExpression = createPropertyPathExpression(part.getProperty());
				// TODO: include factory methods for '= TRUE/FALSE' conditions into spring-data-relational
				return Conditions.isEqual(pathExpression,
						SQL.literalOf((Object) (part.getType() == Part.Type.TRUE ? "TRUE" : "FALSE")));
			}
			case SIMPLE_PROPERTY: {
				Expression pathExpression = createPropertyPathExpression(part.getProperty());
				ParameterMetadata parameterMetadata = parameterMetadataProvider.next(part);
				if (parameterMetadata.isIsNullParameter()) {
					return Conditions.isNull(pathExpression);
				}

				BindMarker bindMarker = createBindMarker(parameterMetadata);
				Expression lhs = upperIfIgnoreCase(part, pathExpression);
				Expression rhs = upperIfIgnoreCase(part, bindMarker, parameterMetadata.getType());
				return Conditions.isEqual(lhs, rhs);
			}
			case NEGATING_SIMPLE_PROPERTY: {
				Expression pathExpression = createPropertyPathExpression(part.getProperty());
				ParameterMetadata parameterMetadata = parameterMetadataProvider.next(part);
				BindMarker bindMarker = createBindMarker(parameterMetadata);
				Expression lhs = upperIfIgnoreCase(part, pathExpression);
				Expression rhs = upperIfIgnoreCase(part, bindMarker, parameterMetadata.getType());
				return Conditions.isEqual(lhs, rhs).not();
			}
			default:
				throw new IllegalArgumentException("Unsupported keyword " + type);
		}

	}

	@NonNull
	private Expression createPropertyPathExpression(PropertyPath propertyPath) {
		RelationalPersistentEntity<?> entity = getPersistentEntity(propertyPath);
		RelationalPersistentProperty property = entity.getRequiredPersistentProperty(propertyPath.getSegment());
		Table table = SQL.table(dataAccessStrategy.toSql(entity.getTableName()));
		return SQL.column(dataAccessStrategy.toSql(property.getColumnName()), table);
	}

	@NonNull
	@SuppressWarnings("unchecked")
	private RelationalPersistentEntity<?> getPersistentEntity(PropertyPath propertyPath) {
		R2dbcConverter converter = dataAccessStrategy.getConverter();
		return converter.getMappingContext().getRequiredPersistentEntity(propertyPath.getOwningType());
	}

	@NonNull
	private BindMarker createBindMarker(ParameterMetadata parameterMetadata) {
		if (parameterMetadata.getName() != null) {
			return SQL.bindMarker(":" + parameterMetadata.getName());
		}
		return SQL.bindMarker();
	}

	/**
	 * Applies an {@code UPPERCASE} conversion to the given {@link Expression} in case the underlying {@link Part}
	 * requires ignoring case.
	 *
	 * @param part method name part (must not be {@literal null})
	 * @param expression expression to be uppercased (must not be {@literal null})
	 * @return uppercased expression or original expression if ignoring case is not strictly required
	 */
	private Expression upperIfIgnoreCase(Part part, Expression expression) {
		return upperIfIgnoreCase(part, expression, part.getProperty().getType());
	}

	/**
	 * Applies an {@code UPPERCASE} conversion to the given {@link Expression} in case the underlying {@link Part}
	 * requires ignoring case.
	 *
	 * @param part method name part (must not be {@literal null})
	 * @param expression expression to be uppercased (must not be {@literal null})
	 * @param expressionType type of the given expression (must not be {@literal null})
	 * @return uppercased expression or original expression if ignoring case is not strictly required
	 */
	private Expression upperIfIgnoreCase(Part part, Expression expression, Class<?> expressionType) {
		switch (part.shouldIgnoreCase()) {
			case ALWAYS:
				Assert.state(canUpperCase(expressionType), "Unable to ignore case of " + expressionType.getName()
						+ " type, the property '" + part.getProperty().getSegment() + "' must reference a string");
				return new Upper(expression);
			case WHEN_POSSIBLE:
				if (canUpperCase(expressionType)) {
					return new Upper(expression);
				}
			case NEVER:
			default:
				return expression;
		}
	}

	private boolean canUpperCase(Class<?> expressionType) {
		return expressionType == String.class;
	}

	// TODO: include support of functions in WHERE conditions into spring-data-relational
	/**
	 * Models the ANSI SQL {@code UPPER} function.
	 * <p>
	 * Results in a rendered function: {@code UPPER(<expression>)}.
	 */
	private class Upper implements Expression {
		private Literal<Object> delegate;

		/**
		 * Creates new instance of this class with the given expression. Only expressions of type {@link Column} and
		 * {@link BindMarker} are supported.
		 *
		 * @param expression expression to be uppercased (must not be {@literal null})
		 */
		private Upper(Expression expression) {
			Assert.notNull(expression, "Expression must not be null!");
			String functionArgument;
			if (expression instanceof BindMarker) {
				functionArgument = expression instanceof Named ? ((Named) expression).getName().getReference()
						: expression.toString();
			} else if (expression instanceof Column) {
				functionArgument = "";
				Table table = ((Column) expression).getTable();
				if (table != null) {
					functionArgument = namingStrategy.getReferenceName(table) + ".";
				}
				functionArgument += namingStrategy.getReferenceName((Column) expression);
			} else {
				throw new IllegalArgumentException("Unable to ignore case expression of type "
                        + expression.getClass().getName() + ". Only " + Column.class.getName() + " and "
                        + BindMarker.class.getName() + " types are supported");
			}
			this.delegate = SQL.literalOf((Object) ("UPPER(" + functionArgument + ")"));
		}

		@Override
		public void visit(Visitor visitor) {
			delegate.visit(visitor);
		}

		@Override
		public String toString() {
			return delegate.toString();
		}
	}

	// TODO: include support of NOT LIKE operator into spring-data-relational
	/**
	 * Negated LIKE {@link Condition} comparing two {@link Expression}s.
	 * <p/>
	 * Results in a rendered condition: {@code <left> NOT LIKE <right>}.
	 */
	private static class NotLike implements Segment, Condition {
		private final Comparison delegate;

		private NotLike(Expression leftColumnOrExpression, Expression rightColumnOrExpression) {
			this.delegate = Comparison.create(leftColumnOrExpression, "NOT LIKE", rightColumnOrExpression);
		}

		/**
		 * Creates new instance of this class with the given {@link Expression}s.
		 *
		 * @param leftColumnOrExpression the left {@link Expression}
		 * @param rightColumnOrExpression the right {@link Expression}
		 * @return {@link NotLike} condition
		 */
		public static NotLike create(Expression leftColumnOrExpression, Expression rightColumnOrExpression) {
			Assert.notNull(leftColumnOrExpression, "Left expression must not be null!");
			Assert.notNull(rightColumnOrExpression, "Right expression must not be null!");
			return new NotLike(leftColumnOrExpression, rightColumnOrExpression);
		}

		@Override
		public void visit(Visitor visitor) {
			Assert.notNull(visitor, "Visitor must not be null!");
			delegate.visit(visitor);
		}

		@Override
		public String toString() {
			return delegate.toString();
		}
	}
}
