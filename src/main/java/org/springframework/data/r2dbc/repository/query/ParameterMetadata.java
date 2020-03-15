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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;

import org.springframework.data.repository.query.parser.Part;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Helper class for holding information about query parameter and preparing query parameter value.
 */
@Builder
@AllArgsConstructor
class ParameterMetadata {
	@Nullable private final String name;
	@NonNull private final Class<?> type;
	@NonNull private final Part.Type partType;
	private final boolean isNullParameter;
	@NonNull private final LikeEscaper likeEscaper;

	/**
	 * Prepares parameter value before it's actually bound to the query.
	 *
	 * @param value must not be {@literal null}
	 * @return prepared query parameter value
	 */
	public Object prepare(Object value) {
		Assert.notNull(value, "Value must not be null!");
		if (String.class.equals(type)) {
			switch (partType) {
				case STARTING_WITH:
					return String.format("%s%%", likeEscaper.escape(value.toString()));
				case ENDING_WITH:
					return String.format("%%%s", likeEscaper.escape(value.toString()));
				case CONTAINING:
				case NOT_CONTAINING:
					return String.format("%%%s%%", likeEscaper.escape(value.toString()));
				default:
					return value;
			}
		}
		return value;
	}

	@Nullable
	public String getName() {
		return name;
	}

	public Class<?> getType() {
		return type;
	}

	/**
	 * Determines whether parameter value should be translated to {@literal IS NULL} condition.
	 *
	 * @return {@literal true} if parameter value should be translated to {@literal IS NULL} condition
	 */
	public boolean isIsNullParameter() {
		return isNullParameter;
	}
}
