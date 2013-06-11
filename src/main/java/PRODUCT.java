import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


public class PRODUCT extends EvalFunc<Double> implements Algebraic {
	
    static public class Initial extends EvalFunc<Tuple> {
        public Tuple exec(Tuple input) throws IOException {
        	return TupleFactory.getInstance().newTuple(prod(input));
        }
    }
    
    static public class Intermed extends EvalFunc<Tuple> {
        public Tuple exec(Tuple input) throws IOException {
        	return TupleFactory.getInstance().newTuple(prod(input));
        }
    }
    
    static public class Final extends EvalFunc<Double> {
        public Double exec(Tuple input) throws IOException {
        	return prod(input);
        }
    }

	public String getFinal() { return Final.class.getName(); }

	public String getInitial() { return Initial.class.getName(); }

	public String getIntermed() { return Intermed.class.getName(); }
	
	static protected Double prod(Tuple in) throws ExecException {
		DataBag values = (DataBag) in.get(0);
		Double result = null;
		
		for (Iterator<Tuple> it = values.iterator(); it.hasNext(); ) {
			Tuple t = it.next();
			
			if (result == null) {
				result = (Double) t.get(0);
			} else {
				result *= (Double) t.get(0);
			}
		}
		
		return result;
	}

	@Override
	public Double exec(Tuple in) throws IOException {
		return prod(in);
	}
	
}