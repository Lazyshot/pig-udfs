
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.map.ObjectMapper;


public class JSONPARSE extends EvalFunc<Map> {

	@Override
	@SuppressWarnings("unchecked")
	public Map exec(Tuple input) throws IOException {
		String in = (String) input.get(0);
		Map<Object, Object> ret = new ObjectMapper().readValue(in, HashMap.class);
		return ret;
	}
	
}