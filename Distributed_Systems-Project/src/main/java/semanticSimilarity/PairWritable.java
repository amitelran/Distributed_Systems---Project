package semanticSimilarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class PairWritable extends Pair<String, String> implements WritableComparable<PairWritable>{
	
    
	public PairWritable(){}
    
	
	
	public PairWritable(String word_i, String word_j) 
	{
		this.first = word_i;
		this.second = word_j;
		
		/*
		int compare = word_i.compareTo(word_j);
		
		// If: 	word_i = "*"	OR 		word_i > word_j (lexicographically)
		
		if (((word_i.equals("*")) || (compare > 0)))
		{
			this.first = word_i;
			this.second = word_j;
		}
		
		// If reached here: 	word_j = "*" 	OR 		word_i <= word_j
		else											
		{
			this.first = word_j;
			this.second = word_i;
		}*/
	}


	public void write(DataOutput out) throws IOException {
		out.writeUTF(first);
		out.writeUTF(second);
	}

	
	public void readFields(DataInput in) throws IOException {
		first = in.readUTF();
		second = in.readUTF();
	}

	
	
	
    public int compareTo(PairWritable other) {
    	int checkArg = this.first.compareTo(other.getFirst());
    	if (checkArg != 0) {
    		return checkArg;
    	}
    	if (this.second.toString().equals("*")) {
    		return -1;
    	}
    	else if (other.getSecond().toString().equals("*")) {
    		return 1;
    	}
    	return (this.second.compareTo(other.getSecond()));
    	
    }
    
    
    
    @Override
    public int hashCode() {
    	int result = 17;
        result = 31 * result + first.hashCode();
        result = 31 * result + second.hashCode();
        return result;
    }
    
    
    
    
    @Override
    public boolean equals(Object other) {
        if (this == other) {
        	return true;
        }
        
        if (other == null || getClass() != other.getClass()) {
        	return false;
        }

		@SuppressWarnings("unchecked")
		Pair<String, String> otherPair = (Pair<String,String>) other;

        if (second != null ? !second.equals(otherPair.getSecond()) : otherPair.getSecond() != null) {
        	return false;
        }
        if (first != null ? !first.equals(otherPair.getFirst()) : otherPair.getFirst() != null) {
        	return false;
        }

        return true;
    }

    
    
    public void orderPairLexicograph() 
	{
    	if (first.equals("*") || second.equals("*") || second.equals("1")) 
    	{
    		return;
    	}
    	
		int compare = first.compareTo(second);
		if (compare > 0) {
			String tmp = this.first;
			this.first = this.second;
			this.second = tmp;
		}
	}


}
