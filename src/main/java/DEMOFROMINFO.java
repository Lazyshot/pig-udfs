import java.io.IOException;
import org.apache.pig.data.DataByteArray;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class DEMOFROMINFO extends EvalFunc<Tuple>
{
	TupleFactory fact = TupleFactory.getInstance();
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		if(input == null || input.size() == 0)
			return null;
		
		@SuppressWarnings("unchecked")
		Map<Object, Object> info = (Map<Object, Object>) input.get(0);
		
		DataByteArray birthdayBytes = (DataByteArray) info.get("birthday");
		
		int years = 0;
		
		if(birthdayBytes == null || birthdayBytes.size() <= 5)
		{
			years = 0;
		}
		else
		{
			DateFormat df = null;
			String birthday = birthdayBytes.toString();

			if(birthday.contains("-")) 
				df = new SimpleDateFormat("yyyy-MM-dd");
			else if(birthday.contains("/"))
				df = new SimpleDateFormat("MM/dd/yyyy");

			if(df != null)
			{
				Date bd;

				try {
					bd = df.parse(birthday);
				} catch (ParseException e) {
					bd = new Date();
					bd.setTime(0);
				}

				Date now = new Date();

				long diff = now.getTime() - bd.getTime();

				years = (int)Math.floor((double)diff / (365.25 * 24 * 60 * 60 * 1000));
			}
		}
		
		Tuple ret = fact.newTuple(2);
		
		String gender = info.containsKey("gender") ? ((DataByteArray) info.get("gender")).toString() : "U";
		
		if(gender.equals(""))
			gender = "U";
		else
			gender = gender.substring(0, 1).toUpperCase();
		
		ret.set(0, gender);
		ret.set(1, years);
		
		return ret;
	}

}