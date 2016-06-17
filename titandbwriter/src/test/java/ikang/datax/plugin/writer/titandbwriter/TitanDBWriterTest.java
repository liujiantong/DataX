package ikang.datax.plugin.writer.titandbwriter;


import org.junit.Test;

import java.util.regex.Pattern;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by liutao on 16/6/17.
 */
public class TitanDBWriterTest {

    @Test
    public void testMobile() {
        assertTrue("15188888888".matches("1\\d{10}"));
        assertFalse("1518888888".matches("1\\d{10}"));
        assertFalse("25188988888".matches("1\\d{10}"));
        assertTrue(Pattern.matches("1\\d{10}", "15188891234"));
    }

}
