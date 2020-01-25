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

import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Helper class to allow easy creation of {@link ParameterMetadata}s.
 * <p>
 * This class is an adapted version of {@code org.springframework.data.jpa.repository.query.ParameterMetadataProvider}
 * from Spring Data JPA project.
 *
 * @author Roman Chigvintsev
 */
class ParameterMetadataProvider {
    private static final Object VALUE_PLACEHOLDER = new Object();

    private final Iterator<? extends Parameter> bindableParameterIterator;
    @Nullable
    private final Iterator<Object> bindableParameterValueIterator;
    private final List<ParameterMetadata> parameterMetadata = new ArrayList<>();

    /**
     * Creates new instance of this class with the given {@link RelationalParameterAccessor}.
     *
     * @param accessor relational parameter accessor (must not be {@literal null}).
     */
    ParameterMetadataProvider(RelationalParameterAccessor accessor) {
        this(accessor.getBindableParameters(), accessor.iterator());
    }

    /**
     * Creates new instance of this class with the given {@link Parameters}.
     *
     * @param parameters method parameters (must not be {@literal null})
     */
    ParameterMetadataProvider(Parameters<?, ?> parameters) {
        this(parameters, null);
    }

    /**
     * Creates new instance of this class with the given {@link Parameters} and {@link Iterator} over all bindable
     * parameter values.
     *
     * @param bindableParameterValueIterator iterator over bindable parameter values
     * @param parameters                     method parameters (must not be {@literal null})
     */
    private ParameterMetadataProvider(Parameters<?, ?> parameters,
                                      @Nullable Iterator<Object> bindableParameterValueIterator) {
        Assert.notNull(parameters, "Parameters must not be null!");
        this.bindableParameterIterator = parameters.getBindableParameters().iterator();
        this.bindableParameterValueIterator = bindableParameterValueIterator;
    }

    /**
     * Creates new instance of {@link ParameterMetadata} for the given {@link Part} and next {@link Parameter}.
     */
    public ParameterMetadata next(Part part) {
        Assert.isTrue(bindableParameterIterator.hasNext(),
                () -> String.format("No parameter available for part %s.", part));
        Parameter parameter = bindableParameterIterator.next();
        String parameterName = getParameterName(parameter);
        Class<?> parameterType = parameter.getType();
        Object parameterValue = getParameterValue();
        boolean isNullProperty = parameterValue == null && Part.Type.SIMPLE_PROPERTY.equals(part.getType());
        return new ParameterMetadata(parameterName, parameterType, isNullProperty);
    }

    @Nullable
    private String getParameterName(Parameter parameter) {
        if (parameter.isExplicitlyNamed()) {
            return parameter.getName().orElseThrow(() -> new IllegalArgumentException("Parameter needs to be named"));
        }
        return null;
    }

    @Nullable
    private Object getParameterValue() {
        return bindableParameterValueIterator == null ? VALUE_PLACEHOLDER : bindableParameterValueIterator.next();
    }

    /**
     * Helper class for holding information about query parameter like parameter name and type.
     */
    static class ParameterMetadata {
        @Nullable
        private final String name;
        private final Class<?> type;
        private final boolean isNullParameter;

        /**
         * Creates new instance of this class with the given method name part, parameter name, parameter type and
         * parameter value.
         *
         * @param name            parameter name
         * @param type            parameter type (must not be {@literal null})
         * @param isNullParameter whether parameter value should be translated to {@literal IS NULL} condition
         */
        public ParameterMetadata(@Nullable String name, Class<?> type, boolean isNullParameter) {
            Assert.notNull(type, "Type must not be null");

            this.name = name;
            this.type = type;
            this.isNullParameter = isNullParameter;
        }

        @Nullable
        public String getName() {
            return name;
        }

        public Class<?> getType() {
            return type;
        }

        public boolean isIsNullParameter() {
            return isNullParameter;
        }
    }
}
