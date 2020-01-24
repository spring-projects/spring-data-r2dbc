package org.springframework.data.r2dbc.repository.query;

import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
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
        return new R2dbcQueryCreator(tree, dataAccessStrategy, entityMetadata, parameterMetadataProvider)
                .createQuery(getDynamicSort(accessor));
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
