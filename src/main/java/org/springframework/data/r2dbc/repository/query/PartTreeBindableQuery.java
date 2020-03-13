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

import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;

/**
 * A {@link BindableQuery} implementation based on a {@link PartTree}.
 *
 * @author Roman Chigvintsev
 * @see PartTreeR2dbcQuery
 */
class PartTreeBindableQuery implements BindableQuery {
	private final String sql;
	private final RelationalParameterAccessor accessor;
	private final ParameterMetadataProvider parameterMetadataProvider;

	/**
	 * Creates new instance of this class with the given SQL query, {@link RelationalParameterAccessor} and
	 * {@link ParameterMetadataProvider}.
	 *
	 * @param sql SQL query (must not be {@literal null} or blank)
	 * @param accessor query parameter accessor (must not be {@literal null})
	 * @param parameterMetadataProvider parameter metadata provider (must not be {@literal null})
	 */
	PartTreeBindableQuery(String sql, RelationalParameterAccessor accessor,
			ParameterMetadataProvider parameterMetadataProvider) {
		Assert.hasText(sql, "SQL query must not be null or blank!");
		Assert.notNull(accessor, "Query parameter accessor must not be null!");
		Assert.notNull(parameterMetadataProvider, "Parameter metadata provider must not be null!");

		this.sql = sql;
		this.accessor = accessor;
		this.parameterMetadataProvider = parameterMetadataProvider;
	}

	@Override
	public <T extends DatabaseClient.BindSpec<T>> T bind(T bindSpec) {
		T bindSpecToUse = bindSpec;

		int index = 0;
		int bindingIndex = 0;

		for (Object value : accessor.getValues()) {
			ParameterMetadata metadata = parameterMetadataProvider.getParameterMetadata(index++);
			String parameterName = metadata.getName();
			Class<?> parameterType = metadata.getType();

			if (parameterName != null) {
				if (value == null) {
					checkNullIsAllowed(metadata, "Value of parameter with name %s must not be null!", parameterName);
					bindSpecToUse = bindSpecToUse.bindNull(parameterName, parameterType);
				} else {
					bindSpecToUse = bindSpecToUse.bind(parameterName, metadata.prepare(value));
				}
			} else {
				if (value == null) {
					checkNullIsAllowed(metadata, "Value of parameter with index %d must not be null!", bindingIndex);
					bindSpecToUse = bindSpecToUse.bindNull(bindingIndex++, parameterType);
				} else {
					bindSpecToUse = bindSpecToUse.bind(bindingIndex++, metadata.prepare(value));
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
}
