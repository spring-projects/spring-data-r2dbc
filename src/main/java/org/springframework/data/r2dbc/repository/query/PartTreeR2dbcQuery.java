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
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.data.r2dbc.core.DatabaseClient.BindSpec;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.repository.query.ParameterMetadataProvider.ParameterMetadata;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.relational.repository.query.RelationalParameters;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.util.Streamable;

/**
 * An {@link AbstractR2dbcQuery} implementation based on a {@link PartTree}.
 * <p>
 * This class is an adapted version of {@code org.springframework.data.jpa.repository.query.PartTreeJpaQuery} from
 * Spring Data JPA project.
 *
 * @author Roman Chigvintsev
 */
public class PartTreeR2dbcQuery extends AbstractR2dbcQuery {
    private final ReactiveDataAccessStrategy dataAccessStrategy;
    private final RelationalParameters parameters;
    private final PartTree tree;

    /**
     * Creates new instance of this class with the given {@link R2dbcQueryMethod} and {@link DatabaseClient}.
     *
     * @param method             query method (must not be {@literal null})
     * @param databaseClient     database client (must not be {@literal null})
     * @param converter          converter (must not be {@literal null})
     * @param dataAccessStrategy data access strategy (must not be {@literal null})
     */
    public PartTreeR2dbcQuery(R2dbcQueryMethod method,
                              DatabaseClient databaseClient,
                              R2dbcConverter converter,
                              ReactiveDataAccessStrategy dataAccessStrategy) {
        super(method, databaseClient, converter);
        this.dataAccessStrategy = dataAccessStrategy;
        this.parameters = method.getParameters();

        try {
            this.tree = new PartTree(method.getName(), method.getEntityInformation().getJavaType());
            validate(this.tree, this.parameters, method.getName());
        } catch (Exception e) {
            String message = String.format("Failed to create query for method %s! %s", method, e.getMessage());
            throw new IllegalArgumentException(message, e);
        }
    }

    @Override
    protected BindableQuery createQuery(RelationalParameterAccessor accessor) {
        RelationalEntityMetadata<?> entityMetadata = getQueryMethod().getEntityInformation();
        ParameterMetadataProvider parameterMetadataProvider = new ParameterMetadataProvider(accessor);
        R2dbcQueryCreator queryCreator = new R2dbcQueryCreator(tree, dataAccessStrategy, entityMetadata,
                parameterMetadataProvider);
        String sql =  queryCreator.createQuery(getDynamicSort(accessor));
        return new BindableQuery() {
            @Override
            public <T extends BindSpec<T>> T bind(T bindSpec) {
                T bindSpecToUse = bindSpec;

                int index = 0;
                int bindingIndex = 0;

                for (Object value : accessor.getValues()) {
                    ParameterMetadata metadata = parameterMetadataProvider.getParameterMetadata(index++);
                    String parameterName = metadata.getName();
                    Class<?> parameterType = metadata.getType();

                    if (parameterName != null) {
                        if (value == null) {
                            checkNullIsAllowed(metadata, "Value of parameter with name %s must not be null!",
                                    parameterName);
                            bindSpecToUse =  bindSpecToUse.bindNull(parameterName, parameterType);
                        } else {
                            bindSpecToUse = bindSpecToUse.bind(parameterName, value);
                        }
                    } else {
                        if (value == null) {
                            checkNullIsAllowed(metadata, "Value of parameter with index %d must not be null!",
                                    bindingIndex);
                            bindSpecToUse = bindSpecToUse.bindNull(bindingIndex++, parameterType);
                        } else {
                            bindSpecToUse = bindSpecToUse.bind(bindingIndex++, value);
                        }
                    }
                }

                return bindSpecToUse;
            }

            @Override
            public String get() {
                return sql;
            }

            private void checkNullIsAllowed(ParameterMetadata metadata, String errorMessage, Object messageArgument) {
                if (!metadata.isIsNullParameter()) {
                    String message = String.format(errorMessage, messageArgument);
                    throw new IllegalArgumentException(message);
                }
            }
        };
    }

    private Sort getDynamicSort(RelationalParameterAccessor accessor) {
        return parameters.potentiallySortsDynamically() ? accessor.getSort() : Sort.unsorted();
    }

    private static void validate(PartTree tree, RelationalParameters parameters, String methodName) {
        int argCount = 0;
        Iterable<Part> parts = () -> tree.stream().flatMap(Streamable::stream).iterator();
        for (Part part : parts) {
            int numberOfArguments = part.getNumberOfArguments();
            for (int i = 0; i < numberOfArguments; i++) {
                throwExceptionOnArgumentMismatch(methodName, part, parameters, argCount);
                argCount++;
            }
        }
    }

    private static void throwExceptionOnArgumentMismatch(String methodName,
                                                         Part part,
                                                         RelationalParameters parameters,
                                                         int index) {
        Part.Type type = part.getType();
        String property = part.getProperty().toDotPath();

        if (!parameters.getBindableParameters().hasParameterAt(index)) {
            String msgTemplate = "Method %s expects at least %d arguments but only found %d. " +
                    "This leaves an operator of type %s for property %s unbound.";
            String formattedMsg = String.format(msgTemplate, methodName, index + 1, index, type.name(), property);
            throw new IllegalStateException(formattedMsg);
        }

        RelationalParameters.RelationalParameter parameter = parameters.getBindableParameter(index);
        if (expectsCollection(type) && !parameterIsCollectionLike(parameter)) {
            String message = wrongParameterTypeMessage(methodName, property, type, "Collection", parameter);
            throw new IllegalStateException(message);
        } else if (!expectsCollection(type) && !parameterIsScalarLike(parameter)) {
            String message = wrongParameterTypeMessage(methodName, property, type, "scalar", parameter);
            throw new IllegalStateException(message);
        }
    }

    private static boolean expectsCollection(Part.Type type) {
        return type == Part.Type.IN || type == Part.Type.NOT_IN;
    }

    private static boolean parameterIsCollectionLike(RelationalParameters.RelationalParameter parameter) {
        return Iterable.class.isAssignableFrom(parameter.getType()) || parameter.getType().isArray();
    }

    private static boolean parameterIsScalarLike(RelationalParameters.RelationalParameter parameter) {
        return !Iterable.class.isAssignableFrom(parameter.getType());
    }

    private static String wrongParameterTypeMessage(String methodName,
                                                    String property,
                                                    Part.Type operatorType,
                                                    String expectedArgumentType,
                                                    RelationalParameters.RelationalParameter parameter) {
        return String.format("Operator %s on %s requires a %s argument, found %s in method %s.", operatorType.name(),
                property, expectedArgumentType, parameter.getType(), methodName);
    }
}
