package org.springframework.data.r2dbc.dialect;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;

/**
 * Unit tests for {@link SqlServerDialect}.
 *
 * @author Mark Paluch
 */
public class SqlServerDialectUnitTests {

	@Test // gh-20
	public void shouldUseNamedPlaceholders() {

		BindMarkers bindMarkers = SqlServerDialect.INSTANCE.getBindMarkersFactory().create();

		BindMarker first = bindMarkers.next();
		BindMarker second = bindMarkers.next("'foo!bar");

		assertThat(first.getPlaceholder()).isEqualTo("@P0");
		assertThat(second.getPlaceholder()).isEqualTo("@P1_foobar");
	}
}
