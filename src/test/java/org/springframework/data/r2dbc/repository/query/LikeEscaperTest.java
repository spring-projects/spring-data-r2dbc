package org.springframework.data.r2dbc.repository.query;

import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author Roman Chigvintsev
 */
public class LikeEscaperTest {
    @Test
    public void ignoresNulls() {
        assertNull(LikeEscaper.DEFAULT.escape(null));
    }
}
