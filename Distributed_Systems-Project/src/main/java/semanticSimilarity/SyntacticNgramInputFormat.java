package semanticSimilarity;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


// InputFormat describes the input-specification for a Map-Reduce job.
// FileInputFormat is a base class for file-based InputFormats

public class SyntacticNgramInputFormat extends FileInputFormat<String, SyntacticNgramLine> {


	// Create a record reader for a given split.
	@Override
	public RecordReader<String, SyntacticNgramLine> createRecordReader(InputSplit split, TaskAttemptContext context) {
		return new SyntacticNgramRecordReader();
	}


	// Is the given filename splitable? Usually, true, but if the file is stream compressed, it will not be.
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
		return codec == null;
	}
}

