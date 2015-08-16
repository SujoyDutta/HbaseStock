
public class StringDouble implements Comparable<StringDouble>{
	
	String str;
	double d;
	public String getStr() {
		return str;
	}
	public void setStr(String str) {
		this.str = str;
	}
	public double getD() {
		return d;
	}
	public void setD(double d) {
		this.d = d;
	}
	public StringDouble(String str, double d) {
		super();
		this.str = str;
		this.d = d;
	}
	@Override
	public int compareTo(StringDouble o) {
		// TODO Auto-generated method stub
		return Double.compare(this.d, o.d);
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return str + "\t" + d;
	}
	

}
