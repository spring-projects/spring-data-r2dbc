package org.springframework.data.r2dbc.repository.query;

import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.r2dbc.repository.query.ParameterMetadataProvider.ParameterMetadata;
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
            case SIMPLE_PROPERTY: {
                PropertyPath propertyPath = part.getProperty();
                @SuppressWarnings("unchecked")
                RelationalPersistentEntity<?> persistentEntity
                        = mappingContext.getRequiredPersistentEntity(propertyPath.getOwningType());
                RelationalPersistentProperty persistentProperty
                        = persistentEntity.getRequiredPersistentProperty(propertyPath.getSegment());

                Table table = SQL.table(persistentEntity.getTableName());
                Column column = SQL.column(persistentProperty.getColumnName(), table);

                ParameterMetadata parameterMetadata = parameterMetadataProvider.next(part);
                if (parameterMetadata.isIsNullParameter()) {
                    return IsNull.create(column);
                }

                BindMarker bindMarker;
                if (parameterMetadata.getName() != null) {
                    bindMarker = SQL.bindMarker(parameterMetadata.getName());
                } else {
                    bindMarker = SQL.bindMarker();
                }
                return Conditions.isEqual(column, bindMarker);
            }
        }
        throw new UnsupportedOperationException("Creating conditions for type " + type + " is unsupported");
    }
}
