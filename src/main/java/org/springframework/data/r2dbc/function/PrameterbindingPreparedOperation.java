/*
 * Copyright 2019 the original author or authors.
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

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;
import org.springframework.data.r2dbc.domain.SettableValue;

import java.util.Map;

/**
 * @author Jens Schauder
 */
public class PrameterbindingPreparedOperation implements PreparedOperation<BindableOperation> {

	private final BindableOperation operation;
	private final Map<String, SettableValue> byName;
	private final Map<Integer, SettableValue> byIndex;

	public PrameterbindingPreparedOperation(BindableOperation operation, Map<String, SettableValue> byName, Map<Integer, SettableValue> byIndex) {

		this.operation = operation;
		this.byName = byName;
		this.byIndex = byIndex;
	}

	@Override
	public BindableOperation getSource() {
		return operation;
	}

	@Override
	public Statement bind(Statement to) {
		return null;
	}

	@Override
	public Statement bind(Connection connection) {

		Statement statement = connection.createStatement(operation.toQuery());

		byName.forEach((name, o) -> {

			if (o.getValue() != null) {
				operation.bind(statement, name, o.getValue());
			} else {
				operation.bindNull(statement, name, o.getType());
			}
		});

		bindByIndex(statement, byIndex);

		return statement;	}

	@Override
	public String toQuery() {
		return null;
	}



	private static void bindByName(Statement statement, Map<String, SettableValue> byName) {

		byName.forEach((name, o) -> {

			if (o.getValue() != null) {
				statement.bind(name, o.getValue());
			} else {
				statement.bindNull(name, o.getType());
			}
		});
	}

	private static void bindByIndex(Statement statement, Map<Integer, SettableValue> byIndex) {

		byIndex.forEach((i, o) -> {

			if (o.getValue() != null) {
				statement.bind(i.intValue(), o.getValue());
			} else {
				statement.bindNull(i.intValue(), o.getType());
			}
		});
	}
}
