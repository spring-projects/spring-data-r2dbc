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

import org.jetbrains.annotations.NotNull;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.*;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.util.Assert;

/**
 * Simple factory to contain logic to create {@link Condition}s from {@link Part}s.
 *
 * @author Roman Chigvintsev
 */
class ConditionFactory {
    private final MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext;
    private final ParameterMetadataProvider parameterMetadataProvider;

    /**
     * Creates new instance of this class with the given {@link MappingContext} and {@link ParameterMetadataProvider}.
     *
     * @param mappingContext            mapping context (must not be {@literal null})
     * @param parameterMetadataProvider parameter metadata provider (must not be {@literal null})
     */
    ConditionFactory(MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext,
                     ParameterMetadataProvider parameterMetadataProvider) {
        Assert.notNull(mappingContext, "Mapping context must not be null");
        Assert.notNull(parameterMetadataProvider, "Parameter metadata provider must not be null");
        this.mappingContext = mappingContext;
        this.parameterMetadataProvider = parameterMetadataProvider;
    }

    /**
     * Creates condition for the given {@link Part}.
     *
     * @param part method name part (must not be {@literal null})
     * @return {@link Condition} instance
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
            case IS_NULL: {
                return Conditions.isNull(createPropertyPathExpression(part.getProperty()));
            }
            case IS_NOT_NULL: {
                return Conditions.isNull(createPropertyPathExpression(part.getProperty())).not();
            }
            case STARTING_WITH:
            case ENDING_WITH:
            case LIKE: {
                Expression pathExpression = createPropertyPathExpression(part.getProperty());
                BindMarker bindMarker = createBindMarker(parameterMetadataProvider.next(part));
                return Conditions.like(pathExpression, bindMarker);
            }
            case NOT_LIKE: {
                Expression pathExpression = createPropertyPathExpression(part.getProperty());
                BindMarker bindMarker = createBindMarker(parameterMetadataProvider.next(part));
                return NotLike.create(pathExpression, bindMarker);
            }
            case SIMPLE_PROPERTY: {
                Expression pathExpression = createPropertyPathExpression(part.getProperty());
                ParameterMetadata parameterMetadata = parameterMetadataProvider.next(part);
                if (parameterMetadata.isIsNullParameter()) {
                    return Conditions.isNull(pathExpression);
                }
                return Conditions.isEqual(pathExpression, createBindMarker(parameterMetadata));
            }
        }
        throw new UnsupportedOperationException("Creating conditions for type " + type + " is unsupported");
    }

    @NotNull
    private Expression createPropertyPathExpression(PropertyPath propertyPath) {
        @SuppressWarnings("unchecked")
        RelationalPersistentEntity<?> persistentEntity
                = mappingContext.getRequiredPersistentEntity(propertyPath.getOwningType());
        RelationalPersistentProperty persistentProperty
                = persistentEntity.getRequiredPersistentProperty(propertyPath.getSegment());
        Table table = SQL.table(persistentEntity.getTableName());
        return SQL.column(persistentProperty.getColumnName(), table);
    }

    @NotNull
    private BindMarker createBindMarker(ParameterMetadata parameterMetadata) {
        if (parameterMetadata.getName() != null) {
            return SQL.bindMarker(parameterMetadata.getName());
        }
        return SQL.bindMarker();
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

        public Expression getLeft() {
            return delegate.getLeft();
        }

        public Expression getRight() {
            return delegate.getRight();
        }

        @Override
        public String toString() {
            return getLeft().toString() + " NOT LIKE " + getRight();
        }
    }
}
