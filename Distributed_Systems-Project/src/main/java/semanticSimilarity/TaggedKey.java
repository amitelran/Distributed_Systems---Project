package semanticSimilarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TaggedKey implements WritableComparable<TaggedKey> {
	
	protected Text key = new Text();
	protected IntWritable tag = new IntWritable();
	
	
	TaggedKey(){}
	
	
	TaggedKey(Text key, IntWritable tag) 
	{
		this.key = key;
		this.tag = tag;
	}
	
	
	
	public int compareTo(TaggedKey taggedKey) 
	{
        int checkArg = this.key.compareTo(taggedKey.getKey());
        if (checkArg != 0 )
        {
        	return checkArg;
        }
       return this.tag.compareTo(taggedKey.getTag());
    }

	


	public IntWritable getTag() { return tag; }
	private Text getKey() { return key; }
	
	
	public void setKey(Text newKey) { this.key = newKey; }
	public void setTag(IntWritable newTag) { this.tag = newTag; }



	public void readFields(DataInput in) throws IOException 
	{
		key.set(in.readUTF());
		tag.set(in.readInt());
	}



	public void write(DataOutput out) throws IOException 
	{
		out.writeUTF(key.toString());
		out.writeInt(tag.get());
	}

}
