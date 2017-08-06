package semanticSimilarity;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;




public class CooccurrencesVectorsHadoop {

	
	/***********************************************************************************************************/
	/******************************************** 	Global counters	 *******************************************/


	public static enum GLOBAL_COUNTERS {
		NUM_OF_LEXEMES,
		NUM_OF_FEATURES,
	};


	/*******************************************************************************************************/
	/******************************************** 	Mapper A	 *******************************************/


	public static class TokenizerMapper extends Mapper<Text, SyntacticNgramLine, Text, IntWritable> {

		private Set<String> goldStandardWords = new HashSet<String>();
		private SyntacticNgram currNgram = new SyntacticNgram();
		private Text token = new Text();
		private IntWritable total_count = new IntWritable();



		/*********** 	Read gold standard dataset words before starting the mapper	 ***********/


		@Override
		protected void setup(Context context) throws IOException, InterruptedException 
		{
			if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
				URI mappingFileUri = context.getCacheFiles()[0];

				if (mappingFileUri != null) {
					readGoldStandardFile("./word-relatedness");
				} 
				else {
					System.out.println(">>>>>> NO MAPPING FILE");
				}
			} 
			else {
				System.out.println(">>>>>> NO CACHE FILES AT ALL");
			}
		}


		/*********** 	Read gold standard dataset file	 ***********/


		private void readGoldStandardFile(String path) {
			try {
				BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
				String wordsPairsLine = null;
				while((wordsPairsLine = bufferedReader.readLine()) != null) 
				{
					String[] splitWords = wordsPairsLine.split("\\t");
					goldStandardWords.add(Stemmer.stemWord(splitWords[0]));
					goldStandardWords.add(Stemmer.stemWord(splitWords[1]));
				}
				bufferedReader.close();
			} 
			catch(IOException ex) {
				System.err.println("Exception while reading gold standard dataset file: " + ex.getMessage());
			}
		}



		/*******************************************	map	 ***********************************************/


		// input: 		key: ngram head word, 						value: syntactic ngram line
		// output: 		key: lexeme | feature | <lexeme, feature>, 	value: total count as indicated in ngram line


		public void map(Text headWord, SyntacticNgramLine ngramLine, Context context) throws IOException,  InterruptedException 
		{
			SyntacticNgram[] ngrams = ngramLine.copyNgrams();			// Extract syntactic ngrams array for multiple uses
			total_count.set(ngramLine.getTotalCount());					// Extract total count integer for multiple uses

			for (int i = 0; i < ngrams.length; i++) 
			{
				currNgram = ngrams[i];
				String currWord = currNgram.getWord();
				String dependancyLabel = currNgram.getDependancyLabel();
				
				SyntacticNgram headIndexNgram = new SyntacticNgram();
				if (currNgram.getHeadIndex() == 0) {
					headIndexNgram = ngrams[currNgram.getHeadIndex()];			// Head word is the current Ngram
				}
				else {
					headIndexNgram = ngrams[currNgram.getHeadIndex() - 1];		// Get the 'head' word which points on current Ngram
				}
				
				String headIndexWord = headIndexNgram.getWord();					

				// If the head-index word is not a part of the gold standard dataset --> no need to build co-occurrences vector
				if (!(goldStandardWords.contains(headIndexWord))) {
					continue;
				}
				
				// If head-index had not been written to context yet --> write, and flag as "written"
				if (!currNgram.isWritten()) 											
				{ 											
					token.set(headIndexWord);											// Send lexeme and total count
					context.write(token, total_count);
					context.getCounter(GLOBAL_COUNTERS.NUM_OF_LEXEMES).increment(total_count.get());
					currNgram.setWritten();
				}
				
				token.set(currWord + "-" + dependancyLabel);							// Send feature (word-dep_label) and total count
				context.write(token, total_count);
				context.getCounter(GLOBAL_COUNTERS.NUM_OF_FEATURES).increment(total_count.get());

				token.set(headIndexWord + "," + currWord + "-" + dependancyLabel);		// Send "lexeme, feature" and total count
				context.write(token, total_count);

			}
		}
	}




	/*******************************************************************************************************/
	/******************************************** 	Reducer A	 *******************************************/


	public static class TokenizerSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {	

		// input: 		key: lexeme | feature | <lexeme, feature>, 		value: Iterable<counts>
		// output: 		key: lexeme | feature | <lexeme, feature>, 		value: summed count


		public void reduce(Text key, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}




	/********************************************************************************************************************************************/




	/*******************************************************************************************************/
	/******************************************** 	Mapper B	 *******************************************/


	public static class PairSplitMapper extends Mapper<Text, IntWritable, PairWritable, Text> {


		private String asterixFlag = "*";


		// input: 		key: lexeme | feature | <lexeme, feature>, 					value: summed count
		// output: 		key: <lexeme, '*'> | <feature, '*'> | <lexeme, "1">, 		value: summed count | <feature, total count lexeme \w feature>
		//                                                              ----> "1" value is not important, just a filler


		public void map(Text token, IntWritable total_count, Context context) throws IOException,  InterruptedException 
		{
			String key = token.toString();
			PairWritable pair;
			Text value = new Text();

			// Case token is of form: "lexeme,word-dep_label" --> split to make lexeme the key, and add feature to the value
			if (key.contains(",")) 
			{
				String[] split = key.split(",");
				pair = new PairWritable(split[0], "1");					// create pair: <lexeme, "1">   ("1" value is not really important)
				value.set(split[1] + "," + total_count.toString());  	// create: "feature, count" as value
			}

			// Case token is lexeme only or feature only
			else 
			{
				pair = new PairWritable(key, asterixFlag);				// create pair: <lexeme, '*'> or <feature, '*'>
				value.set(total_count.toString());						// create value: total_count
			}
			context.write(pair, value);
		}
	}



	/*******************************************************************************************************/
	/********************************** 	Partitioner for MapReduce B	 **************************************/


	// Custom partitioner to ensure pairs of the same left element are partitioned to the same reducer.
	// Thus, making pair of the form <element, "*"> and <element, "1"> reach the same reducer


	public static class PairWritablePartitioner extends Partitioner<PairWritable, Text> {

		@Override
		public int getPartition(PairWritable keyPair, Text value, int numPartitions) 
		{
			return (keyPair.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}



	/*******************************************************************************************************/
	/******************************************** 	Reducer B	 *******************************************/



	public static class FeatureInfoReducer extends Reducer<PairWritable, Text, PairWritable, Text> {	


		private String totalLexemeCount;
		private String lexeme;

		// input: 		key: 	<feature, '*'> | <lexeme, '*'> | <lexeme, '1'>, 		
		//				value:  Iterable<summed count> 		   | Iterable<<feature, total count lexeme & feature>>
		
		// output: 		key: 	<feature, '*'> 		   | <feature, '1'>,		
		//				value: 	total count of feature | <lexeme, lexeme&feature count, lexeme's total count>


		public void reduce(PairWritable keyPair, Iterable<Text> countsOrFeatures, Context context) throws IOException, InterruptedException 
		{
			int sum = 0;

			// Case of <something, '*'>: '*' pair should be first in order
			if (keyPair.getSecond().equals("*")) 						
			{
				// Case the input key is of a feature (for example: "dog-subj") --> pass as is for future MapReduce phases
				if (keyPair.getFirst().toString().contains("-")) 
				{
					for (Text value : countsOrFeatures) 
					{
						sum = Integer.parseInt(value.toString());
						break;
					}
					context.write(keyPair, new Text(Integer.toString(sum)));	// Simply write as it is
				}

				// Case the input key is of a lexeme --> Get the total lexeme count for future uses
				else 
				{
					for (Text value : countsOrFeatures) 
					{
						totalLexemeCount = value.toString();
						break;
					}
				}

			}

			// case of: <lexeme, '1'> --> write <featureName, "1"> as key, "lexeme,lexeme & feature count, totalLexemeCount"
			else 
			{
				lexeme = keyPair.getFirst().toString();
				for (Text value : countsOrFeatures) 
				{
					String[] split = value.toString().split(",");			// split[0] = feature, split[1] = lexeme & feature count
					context.write(new PairWritable(split[0], "1"), new Text(lexeme + "," + split[1] + "," + totalLexemeCount));	// value: <lexeme, lexeme&feature count, lexeme's total count>
				}
			}	
		}
	}




/********************************************************************************************************************************************/



	/*******************************************************************************************************/
	/******************************************** 	Mapper C	 *******************************************/

	// Passes data on to the reducer


	public static class IdentityMapper extends Mapper<PairWritable, Text, PairWritable, Text> {

		// input: 		key: <feature, '*'> | <feature, '1'>, 			value: summed count | <lexeme, lexeme&feature count, lexeme's total count>
		// output: 		key: <feature, '*'> | <feature, '1'>, 			value: summed count | <lexeme, lexeme&feature count, lexeme's total count>


		public void map(PairWritable key, Text value, Context context) throws IOException,  InterruptedException 
		{
			context.write(key, value);
		}
	}

	

	/*******************************************************************************************************/
	/******************************************** 	Reducer C	 *******************************************/


	public static class MeasuresOfAssocWithContextReducer extends Reducer<PairWritable, Text, Text, Feature> {

		public int totalFeatureCount = 0;
		public double totalLexemesCorpus = 0;
		public double totalFeaturesCorpus = 0;


		/*******************************************	Setup to get Global Counters values	 ***********************************************/


		@Override
		public void setup(Context context) throws IOException, InterruptedException
		{	
			totalLexemesCorpus = (double) context.getConfiguration().getLong("totalLexemesCorpus", 0);
			totalFeaturesCorpus = (double) context.getConfiguration().getLong("totalFeaturesCorpus", 0);
		}


		/*******************************************	Reduce C	 ***********************************************/


		// input: 		key: 	<feature, '*'> 		   | <feature, '1'>, 	
		//				value: 	Iterable<summed count> | Iterable<<lexeme, lexeme&feature count, lexeme's total count>>
		
		// output: 		key: 	feature_name, 							
		//				value:  Feature data structure containing all computed measures


		
		public void reduce(PairWritable featureKey, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			
			// Case of <feature, '*'>: We expect '*' pair to be the first in order
			if (featureKey.getSecond().equals("*")) 			
			{
				for (Text value : values) 
				{
					totalFeatureCount = Integer.parseInt(value.toString());
					break;
				}
			}

			// Case of <feature, '1'>: 	value is of triplet form:	 <lexeme, lexeme&feature count, lexeme's total count>
			else 
			{
				for (Text value : values) 
				{
					String[] split = value.toString().split(",");	// split[0] = lexeme, split[1] = lexeme&feature count, split[2] = lexeme's total count
					if (split.length < 3) {
						continue;
					}
					Feature feature = new Feature(featureKey.getFirst(), split[0], totalFeatureCount, Integer.parseInt(split[2]));
					feature.computeAllMeasures(Integer.parseInt(split[1]), totalLexemesCorpus, totalFeaturesCorpus);
					context.write(new Text(featureKey.getFirst()), feature);			// output: feature_name, featureData
				}
			}
		}
	}
	
	
	// End of phase MapReduce C: 
	//			At this stage we should hold all of the features as keys, and all of their data, including computed 
	//			measures, as values.
	//			Now, we want to compute the vectors similarities, as vectors are composed of the lexeme's features.
	//			Thus, we will construct the co-occurrences vectors for each lexeme.
	
	
	
/********************************************************************************************************************************************/

	
	
	
	/*******************************************************************************************************/
	/******************************************** 	Mapper D	 *******************************************/

	
	// We will want to be able to construct co-occurrences vectors.
	// Thus, will need to make each 'lexeme' as a key, and send the lexeme's features as values, enabling the 
	// reducer to get all of the lexeme's features as values.
	// This will enable us to construct the lexeme's co-occurrences vector
	
	
	public static class VectorConstructorMapper extends Mapper<Text, Feature, Text, Feature> {

		
		// input: 		key: feature_name, 			value: Feature data structure
		// output: 		key: lexeme, 				value: Feature data structure


		public void map(Text featureKey, Feature featureValue, Context context) throws IOException,  InterruptedException 
		{
			context.write(new Text(featureValue.getLexeme()), featureValue);
		}
	}
	
		
	
	/*******************************************************************************************************/
	/******************************************** 	Reducer D 	 *******************************************/


	
	public static class VectorConstructorReducer extends Reducer<Text, Feature, Text, CooccurrencesVector> 
	{

		// input: 		key: lexeme_name, 	value: Iterable<Features>
		// output: 		key: lexeme_name, 	value: Lexeme's Co-occurrences vector


		public void reduce(Text lexemeName, Iterable<Feature> features, Context context) throws IOException, InterruptedException 
		{
			CooccurrencesVector vector = new CooccurrencesVector(lexemeName.toString());
			for (Feature feature : features) 								// Add the all of the lexemes features to the vector
			{
				vector.addFeature(feature);
			}
			vector.computeVectorNorms();									// Compute vector's norms (for all four measures of association with context)
			context.write(lexemeName, vector);
		}
	}
	
	
	
	
	/*******************************************************************************************************/
	/******************************************** 	Mapper E	 *******************************************/


	
	public static class VectorsSimMapper extends Mapper<Text, CooccurrencesVector, Text, CooccurrencesVector> {
				
		private Set<String> goldStandardWords = new HashSet<String>();		
		

		/*********** 	Setup - Read gold standard dataset words before starting the mapper	 ***********/


		@Override
		protected void setup(Context context) throws IOException, InterruptedException 
		{
			if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) 
			{
				URI mappingFileUri = context.getCacheFiles()[0];

				if (mappingFileUri != null) {
					readGoldStandardFile("./word-relatedness");
				} 
				else {
					System.out.println(">>>>>> NO MAPPING FILE");
				}
			} 
			else {
				System.out.println(">>>>>> NO CACHE FILES AT ALL");
			}
		}


		/*********** 	Read gold standard dataset file	 ***********/


		private void readGoldStandardFile(String path) 
		{
			try {
				BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
				String wordsPairsLine = null;
				while((wordsPairsLine = bufferedReader.readLine()) != null) 
				{
					String[] splitWords = wordsPairsLine.split("\\t");
					goldStandardWords.add(Stemmer.stemWord(splitWords[0]));
					goldStandardWords.add(Stemmer.stemWord(splitWords[1]));
				}
				bufferedReader.close();
			} 
			catch(IOException ex) {
				System.err.println("Exception while reading gold standard dataset file: " + ex.getMessage());
			}
		}
		
		
		/*******************************************	Vectors Similarities Map	 ***********************************************/
		
		
		// Idea is to generate key pairs of <lexeme_i, lexeme_j> and send one of lexeme's vector as value.
		// The reducer will get <lexeme_i, lexeme_j> pairs, and Iterable<vector> containing exactly both the lexemes' vectors.
		// Since <lexeme_i, lexeme_j> might appear also as <lexeme_j, lexeme_i>, we will make sure both the cases are
		// sorted in the same way (by lexicographical order)
		
		
		// input: 		key: lexeme_name, 						value: Co-occurrences vector
		// output: 		key: <lexeme_name, otherLexeme_name>, 	value: Co-occurrences vector

		
		public void map(Text lexeme_name, CooccurrencesVector vector, Context context) throws IOException,  InterruptedException 
		{	
			String lexeme = lexeme_name.toString();
			
			for (String otherLexeme : goldStandardWords)		// Send all <lexeme, otherLexeme> pairs, besides the same lexeme pair
			{
				if (otherLexeme.equals(lexeme)) {				// Don't send same lexeme pairs
					continue;
				}
				context.write(new Text(orderPairLexicograph(lexeme, otherLexeme)), vector);		// Sort pair lexicographically to ensure same key
			}
		}
		
		
		/*******************************************	Order words lexicographically	 ***********************************************/
		
		
		public String orderPairLexicograph(String word1, String word2) 
		{
			int compare = word1.compareTo(word2);
			if (compare > 0) {					// If positive: word2 < word1 lexicographically
				return (word2 + "," + word1);
			}
			else {								// word1 > word2
				return (word1 + "," + word2);
			}
		}
	}


		
	
	
	/*******************************************************************************************************/
	/*********************************** 	Reducer E - Vectors Similarities generator	 *******************************/
	
	
	
	public static class VectorsSimReducer extends Reducer<PairWritable, CooccurrencesVector, Text, Text> {
		
		
		public Map<String, String> goldStandardPairs = new HashMap<String, String>();


		
		/*********** 	Read gold standard dataset words before starting the mapper	 ***********/


		@Override
		protected void setup(Context context) throws IOException, InterruptedException 
		{
			if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
				URI mappingFileUri = context.getCacheFiles()[0];

				if (mappingFileUri != null) {
					mapGoldStandardFile("./word-relatedness");
				} 
				else {
					System.out.println(">>>>>> NO MAPPING FILE");
				}
			} 
			else {
				System.out.println(">>>>>> NO CACHE FILES AT ALL");
			}
		}


		/*********** 	Read gold standard dataset file	 ***********/


		public void mapGoldStandardFile(String path) 
		{
			try {
				BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
				String wordsPairsLine = null;
				while((wordsPairsLine = bufferedReader.readLine()) != null) 
				{
					String[] splitWords = wordsPairsLine.split("\\t");
					String key = (Stemmer.stemWord(splitWords[0]) + Stemmer.stemWord(splitWords[1]));
					goldStandardPairs.put(key, splitWords[2]);
				}
				bufferedReader.close();
			} 
			catch(IOException ex) {
				System.err.println("Exception while reading gold standard dataset file in mapGoldStandardFile: " + ex.getMessage());
			}
		}
		

		
		/*********** 	Reduce E	 ***********/

		
		// input: 		key: 	<lexeme_i, lexeme_j>, 	value: Iterable<CooccurrencesVector>		
		// output: 		key: 	<lexeme_i, lexeme_j> 	value: Vectors similarities as text

	
		public void reduce(Text lexemesPair, Iterable<CooccurrencesVector> vectors, Context context) throws IOException, InterruptedException 
		{
			String key = lexemesPair.toString();
			String[] splitKey = key.split(",");
			String leftInPair = splitKey[0];
			String rightInPair = splitKey[1];
			String similarity = goldStandardPairs.get(key);		// Pair is already sorted lexicographically
			boolean leftExists = false;
			boolean rightExists = false;
			CooccurrencesVector leftVector = new CooccurrencesVector();
			CooccurrencesVector rightVector = new CooccurrencesVector();
			
			for (CooccurrencesVector vector : vectors)			// Get both vectors
			{
				if (vector.getLexeme().equals(leftInPair))			// Set left vector
				{
					leftExists = true;
					leftVector = new CooccurrencesVector(vector); 
				}
				else if (vector.getLexeme().equals(rightInPair))	// Set right vector
				{
					rightExists = true;
					rightVector = new CooccurrencesVector(vector); 
				}
			}
			
			// Ensure both the lexemes exist in corpus.
			// (since mapper sent all pairs from gold standard, which some might not appear in our corpus)
			
			if (leftExists && rightExists) 				
			{
				VectorsSimilaritiesWritable vectorSim = leftVector.vectorsSim(rightVector, similarity);	// Compute similarities and write to output
				context.write(lexemesPair, new Text(vectorSim.toString()));
			}
		}
	}
	

	
	
	
	
	
	/*******************************************************************************************************/
	/******************************************** 	MAIN	 **********************************************/
	
	
	
	public static void main(String[] args) throws Exception 
	{
		
		if(args.length < 3) {
			System.err.println("Not enough arguments. needed <InputPath> <OutputPath> <GoldStandardDatasetFilePath>");
			System.exit(1);
		}
		
		
		Configuration conf = new Configuration();

		
		/***********************************	Job 1 - MapReduce A	 *****************************************/

		
		Job job1 = Job.getInstance(conf, "Tokens Count");
		job1.setJarByClass(CooccurrencesVectorsHadoop.class);
		
		job1.setMapperClass(TokenizerMapper.class);
		job1.setCombinerClass(TokenizerSumReducer.class);
		job1.setReducerClass(TokenizerSumReducer.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		job1.setInputFormatClass(SyntacticNgramInputFormat.class);
		SyntacticNgramInputFormat.addInputPath(job1, new Path(args[0]));
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job1, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputPath(job1, new Path(args[1]+".intermediate1"));
		SequenceFileOutputFormat.setOutputCompressorClass(job1, GzipCodec.class);
		
		job1.addCacheFile(new URI(args[2] + "#word-relatedness"));
		
		if(!job1.waitForCompletion(true))
			System.exit(1);
		
		
		// Get global counters: counter for total appearances of lexemes in corpus, counter for total appearances of features in corpus
		long totalLexemesCorpus = job1.getCounters().findCounter(GLOBAL_COUNTERS.NUM_OF_LEXEMES).getValue();		// Get overall counter of all lexemes
		long totalFeaturesCorpus = job1.getCounters().findCounter(GLOBAL_COUNTERS.NUM_OF_FEATURES).getValue();		// Get overall counter of all

		
		/***********************************	Job 2 - MapReduce B	 *****************************************/
		
		
		//conf.setLong("numOfTweets", job1.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue());

		Job job2 = Job.getInstance(conf, "Gather Features Data");
		job2.setJarByClass(CooccurrencesVectorsHadoop.class);
		
		job2.setMapperClass(PairSplitMapper.class);
		job2.setPartitionerClass(PairWritablePartitioner.class);
		job2.setReducerClass(FeatureInfoReducer.class);
		
		job2.setMapOutputKeyClass(PairWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(PairWritable.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job2, new Path(args[1]+".intermediate1"));
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job2, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputPath(job2, new Path(args[1]+".intermediate2"));
		SequenceFileOutputFormat.setOutputCompressorClass(job2, GzipCodec.class);

		if(!job2.waitForCompletion(true))
			System.exit(1);
		
		
		/***********************************	Job 3 - MapReduce C	 *****************************************/

		
		Job job3 = Job.getInstance(conf, "Measures of Association with Context");
		job3.setJarByClass(CooccurrencesVectorsHadoop.class);
		
		job3.setMapperClass(IdentityMapper.class);
		job3.setPartitionerClass(PairWritablePartitioner.class);
		job3.setReducerClass(MeasuresOfAssocWithContextReducer.class);
		
		job3.getConfiguration().setLong("totalLexemesCorpus", totalLexemesCorpus);				// Set global counters data for MapReduce C
		job3.getConfiguration().setLong("totalFeaturesCorpus", totalFeaturesCorpus);			// Set global counters data for MapReduce C
		
		job3.setMapOutputKeyClass(PairWritable.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Feature.class);
		
		job3.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job3, new Path(args[1]+".intermediate2"));
		job3.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job3, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputPath(job3, new Path(args[1]+".intermediate3"));
		SequenceFileOutputFormat.setOutputCompressorClass(job3, GzipCodec.class);
		
		//FileOutputFormat.setOutputPath(job3, new Path(args[1]+".intermediate3"));


		if(!job3.waitForCompletion(true))
			System.exit(1);
		
		
		/***********************************	Job 4 - MapReduce D	 *****************************************/

		
		Job job4 = Job.getInstance(conf, "Vectors construction");
		job4.setJarByClass(CooccurrencesVectorsHadoop.class);
		
		job4.setMapperClass(VectorConstructorMapper.class);
		job4.setReducerClass(VectorConstructorReducer.class);
		
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(Feature.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(CooccurrencesVector.class);
		
		job4.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job4, new Path(args[1]+".intermediate3"));
		job4.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job4, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputPath(job4, new Path(args[1]+".intermediate4"));
		SequenceFileOutputFormat.setOutputCompressorClass(job4, GzipCodec.class);

		if(!job4.waitForCompletion(true))
			System.exit(1);
		
		
		/***********************************	Job 5 - MapReduce E	 *****************************************/
		
		
		Job job5 = Job.getInstance(conf, "Vectors Similiarities");
		job5.setJarByClass(CooccurrencesVectorsHadoop.class);
		
		job5.setMapperClass(VectorsSimMapper.class);
		job5.setReducerClass(VectorsSimReducer.class);
		
		job5.setMapOutputKeyClass(Text.class);
		job5.setMapOutputValueClass(CooccurrencesVector.class);
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(Text.class);
		
		job5.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job5, new Path(args[1]+".intermediate4"));
		FileOutputFormat.setOutputPath(job5, new Path(args[1]));
		
		job5.addCacheFile(new URI(args[2] + "#word-relatedness"));
		
		if(!job5.waitForCompletion(true))
			System.exit(1);
		
		
		/***********************************	Print MapReduce jobs stats	 *****************************************/
		
		
		Job[] jobs = { job1, job2, job3, job4, job5};
		for (int i = 0; i < jobs.length; i++) 
		{
			System.out.println("Phase " + (i+1) + " Key-Value Pairs:");
			System.out.println("\tPairs sent from Mapper: " + jobs[i].getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue());
	        System.out.println("\tTotal size: " + jobs[i].getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue() + " bytes");			
		}
	
	}
}









