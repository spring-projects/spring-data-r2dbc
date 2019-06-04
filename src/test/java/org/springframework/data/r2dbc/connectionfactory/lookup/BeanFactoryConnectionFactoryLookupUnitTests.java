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
package org.springframework.data.r2dbc.connectionfactory.lookup;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.ConnectionFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanNotOfRequiredTypeException;

/**
 * Unit tests for {@link BeanFactoryConnectionFactoryLookup}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class BeanFactoryConnectionFactoryLookupUnitTests {

	private static final String CONNECTION_FACTORY_BEAN_NAME = "connectionFactory";

	@Mock BeanFactory beanFactory;

	@Test // gh-98
	public void shouldLookupConnectionFactory() {

		DummyConnectionFactory expectedConnectionFactory = new DummyConnectionFactory();
		when(beanFactory.getBean(CONNECTION_FACTORY_BEAN_NAME, ConnectionFactory.class))
				.thenReturn(expectedConnectionFactory);

		BeanFactoryConnectionFactoryLookup lookup = new BeanFactoryConnectionFactoryLookup();
		lookup.setBeanFactory(beanFactory);

		ConnectionFactory connectionFactory = lookup.getConnectionFactory(CONNECTION_FACTORY_BEAN_NAME);

		assertThat(connectionFactory).isNotNull();
		assertThat(connectionFactory).isSameAs(expectedConnectionFactory);
	}

	@Test // gh-98
	public void shouldLookupWhereBeanFactoryYieldsNonConnectionFactoryType() {

		BeanFactory beanFactory = mock(BeanFactory.class);

		when(beanFactory.getBean(CONNECTION_FACTORY_BEAN_NAME, ConnectionFactory.class)).thenThrow(
				new BeanNotOfRequiredTypeException(CONNECTION_FACTORY_BEAN_NAME, ConnectionFactory.class, String.class));

		BeanFactoryConnectionFactoryLookup lookup = new BeanFactoryConnectionFactoryLookup(beanFactory);

		assertThatExceptionOfType(ConnectionFactoryLookupFailureException.class)
				.isThrownBy(() -> lookup.getConnectionFactory(CONNECTION_FACTORY_BEAN_NAME));
	}

	@Test // gh-98
	public void shouldLookupWhereBeanFactoryHasNotBeenSupplied() {

		BeanFactoryConnectionFactoryLookup lookup = new BeanFactoryConnectionFactoryLookup();

		assertThatThrownBy(() -> lookup.getConnectionFactory(CONNECTION_FACTORY_BEAN_NAME))
				.isInstanceOf(IllegalStateException.class);
	}
}
