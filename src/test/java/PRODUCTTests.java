
import static org.junit.Assert.*;

import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.junit.Test;

public class PRODUCTTests extends PigTests {
	@Test
	public void prodtestDouble() throws Exception
	{
		PigTest test = createPigTest("src/test/resources/product.pig");
		
		String[] input = {"1.0", "1.0", "2.0", "5.0", "3.0", "1.1"};
		writeLinesToFile("input", input);
		
		test.runScript();
		
		List<Tuple> output = getLinesForAlias(test, "data_out", true);
		
		assertEquals(1, output.size());
		assertEquals("(33.0)", output.get(0).toString());
	}
}