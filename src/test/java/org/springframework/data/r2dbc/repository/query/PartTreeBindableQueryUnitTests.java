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

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.any;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;

/**
 * @author Roman Chigvintsev
 */
@RunWith(MockitoJUnitRunner.class)
public class PartTreeBindableQueryUnitTests {
	@Mock private RelationalParameterAccessor parameterAccessor;
	@Mock private ParameterMetadataProvider parameterMetadataProvider;
	@Mock private ParameterMetadata parameterMetadata;

	@Rule public ExpectedException thrown = ExpectedException.none();

	@SuppressWarnings("ResultOfMethodCallIgnored")
	@Before
	public void setUp() {
		doReturn(String.class).when(parameterMetadata).getType();
		when(parameterMetadata.prepare(any())).thenCallRealMethod();
		when(parameterMetadataProvider.getParameterMetadata(anyInt())).thenReturn(parameterMetadata);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void bindsNonNullValueToQueryParameterByIndex() {
		when(parameterAccessor.getValues()).thenReturn(new Object[] { "test" });
		DatabaseClient.BindSpec bindSpecMock = mock(DatabaseClient.BindSpec.class);

		PartTreeBindableQuery query = new PartTreeBindableQuery("SELECT", parameterAccessor, parameterMetadataProvider);
		query.bind(bindSpecMock);
		verify(bindSpecMock, times(1)).bind(0, "test");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void bindsNonNullValueToQueryParameterByName() {
		when(parameterAccessor.getValues()).thenReturn(new Object[] { "test" });
		when(parameterMetadata.getName()).thenReturn("param0");
		DatabaseClient.BindSpec bindSpecMock = mock(DatabaseClient.BindSpec.class);

		PartTreeBindableQuery query = new PartTreeBindableQuery("SELECT", parameterAccessor, parameterMetadataProvider);
		query.bind(bindSpecMock);
		verify(bindSpecMock, times(1)).bind("param0", "test");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void bindsNullValueToQueryParameterByIndex() {
		when(parameterAccessor.getValues()).thenReturn(new Object[1]);
		when(parameterMetadata.isIsNullParameter()).thenReturn(true);
		DatabaseClient.BindSpec bindSpecMock = mock(DatabaseClient.BindSpec.class);

		PartTreeBindableQuery query = new PartTreeBindableQuery("SELECT", parameterAccessor, parameterMetadataProvider);
		query.bind(bindSpecMock);
		verify(bindSpecMock, times(1)).bindNull(0, String.class);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void bindsNullValueToQueryParameterByName() {
		when(parameterAccessor.getValues()).thenReturn(new Object[1]);
		when(parameterMetadata.getName()).thenReturn("param0");
		when(parameterMetadata.isIsNullParameter()).thenReturn(true);
		DatabaseClient.BindSpec bindSpecMock = mock(DatabaseClient.BindSpec.class);

		PartTreeBindableQuery query = new PartTreeBindableQuery("SELECT", parameterAccessor, parameterMetadataProvider);
		query.bind(bindSpecMock);
		verify(bindSpecMock, times(1)).bindNull("param0", String.class);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void throwsExceptionWhenNullIsNotAllowedAsIndexedQueryParameter() {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Value of parameter with index 0 must not be null!");

		when(parameterAccessor.getValues()).thenReturn(new Object[1]);
		DatabaseClient.BindSpec bindSpecMock = mock(DatabaseClient.BindSpec.class);

		PartTreeBindableQuery query = new PartTreeBindableQuery("SELECT", parameterAccessor, parameterMetadataProvider);
		query.bind(bindSpecMock);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void throwsExceptionWhenNullIsNotAllowedAsNamedQueryParameter() {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Value of parameter with name param0 must not be null!");

		when(parameterAccessor.getValues()).thenReturn(new Object[1]);
		when(parameterMetadata.getName()).thenReturn("param0");
		DatabaseClient.BindSpec bindSpecMock = mock(DatabaseClient.BindSpec.class);

		PartTreeBindableQuery query = new PartTreeBindableQuery("SELECT", parameterAccessor, parameterMetadataProvider);
		query.bind(bindSpecMock);
	}
}
