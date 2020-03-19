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

import org.springframework.data.repository.query.parser.Part;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Helper class for holding information about query parameter and preparing query parameter value.
 */
class ParameterMetadata {
	private final String name;
	private final Class<?> type;
	private final Part.Type partType;
	private final boolean isNullParameter;
	private final LikeEscaper likeEscaper;

	private ParameterMetadata(Builder builder) {
		Assert.notNull(builder.type, "Parameter type must not be null");
		Assert.notNull(builder.partType, "Parameter part type must not be null");
		Assert.notNull(builder.likeEscaper, "Like escaper must not be null");
		
		this.name = builder.name;
		this.type = builder.type;
		this.partType = builder.partType;
		this.isNullParameter = builder.isNullParameter;
		this.likeEscaper = builder.likeEscaper;
	}

	/**
	 * Creates new instance of {@link Builder}.
	 *
	 * @return new instance of {@link Builder}
	 */
	public static Builder builder() {
		return new Builder();
	}

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
					return likeEscaper.escape(value.toString()) + "%";
				case ENDING_WITH:
					return "%" + likeEscaper.escape(value.toString());
				case CONTAINING:
				case NOT_CONTAINING:
					return "%" + likeEscaper.escape(value.toString()) + "%";
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

	/**
	 * Parameter metadata builder.
	 */
	public static class Builder {
		private String name;
		private Class<?> type;
		private Part.Type partType;
		private boolean isNullParameter;
		private LikeEscaper likeEscaper;

		public Builder name(String name) {
			this.name = name;
			return this;
		}

		public Builder type(Class<?> type) {
			this.type = type;
			return this;
		}

		public Builder partType(Part.Type partType) {
			this.partType = partType;
			return this;
		}

		public Builder isNullParameter(boolean isNullParameter) {
			this.isNullParameter = isNullParameter;
			return this;
		}

		public Builder likeEscaper(LikeEscaper likeEscaper) {
			this.likeEscaper = likeEscaper;
			return this;
		}

		public ParameterMetadata build() {
			return new ParameterMetadata(this);
		}
	}
}
