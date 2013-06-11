
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class PAGESMAPTOARRAY extends EvalFunc<DataBag> {

	@Override
	@SuppressWarnings("unchecked")
	public DataBag exec(Tuple input) throws IOException {
		TupleFactory tupfact = TupleFactory.getInstance();
		Map<Object, Object> map = (Map<Object, Object>) input.get(0);
		DefaultDataBag bag = new DefaultDataBag();
		
		for(Entry<Object, Object> ent: map.entrySet())
			bag.add(tupfact.newTuple(ent.getKey()));
		
		return bag;
	}
	
}