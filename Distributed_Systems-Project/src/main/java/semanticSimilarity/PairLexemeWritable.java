package semanticSimilarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class PairLexemeWritable implements WritableComparable<PairWritable> {
	
	protected String first;
	protected String second;
	
	
	/*********** 	Default Constructor	 ***********/

    
	public PairLexemeWritable(){}
    
	
	/*********** 	Constructor	by two Strings  ***********/

	
	public PairLexemeWritable(String word_i, String word_j) 
	{
		this.first = word_i;
		this.second = word_j;
	}

	
	/*********** 	Read & Write methods	 ***********/


	
	public void readFields(DataInput in) throws IOException 
	{
		first = in.readUTF();
		second = in.readUTF();
	}
	
	
	
	public void write(DataOutput out) throws IOException 
	{
		out.writeUTF(first);
		out.writeUTF(second);
	}

	
	
	/*********** 	CompareTo	 ***********/
	
	
    public int compareTo(PairWritable other) 
    {
    	int checkArg = this.first.compareTo(other.getFirst());
    	if (checkArg != 0) {
    		return checkArg;
    	}
    	return (this.second.compareTo(other.getSecond()));
    	
    }
    
    
    
	/*********** 	hashCode	 ***********/

    
    
    @Override
    public int hashCode() {
    	int result = 17;
        result = 31 * result + first.hashCode();
        result = 31 * result + second.hashCode();
        return result;
    }
    
    
    
	/*********** 	equals	 ***********/

    
    @Override
    public boolean equals(Object other) {
        if (this == other) {
        	return true;
        }
        
        if (other == null || getClass() != other.getClass()) {
        	return false;
        }

		PairWritable otherPair = (PairWritable) other;

        if (second != null ? !second.equals(otherPair.getSecond()) : otherPair.getSecond() != null) {
        	return false;
        }
        if (first != null ? !first.equals(otherPair.getFirst()) : otherPair.getFirst() != null) {
        	return false;
        }

        return true;
    }

    
    
	/*********** 	Order pair elements by lexicographical order	 ***********/

    // We will use it to ensure <i, j> is equal to <j, i>, by sorting --> <i, j>
    
    
    public void orderPairLexicograph() 
	{
    	if (first.equals("*") || second.equals("*") || second.equals("1")) 
    	{
    		return;
    	}
    	
		int compare = first.compareTo(second);
		if (compare > 0) 
		{
			String tmp = this.first;
			this.first = this.second;
			this.second = tmp;
		}
	}
    
    
    public void setFirst(String first) {
        this.first = first;
    }

    public void setSecond(String second) {
        this.second = second;
    }

    public String getFirst() {
        return first;
    }

    public String getSecond() {
        return second;
    }
    
    
    public String toString() {
    	return ("<" + this.first + "," + this.second + ">");
    }


}
