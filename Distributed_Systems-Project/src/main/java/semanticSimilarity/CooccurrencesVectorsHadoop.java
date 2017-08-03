package semanticSimilarity;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
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
		Stemmer stemmer = new Stemmer();
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
					String word1 = Stemmer.stemWord(splitWords[0]);
					String word2 = Stemmer.stemWord(splitWords[1]);
					goldStandardWords.add(word1);
					goldStandardWords.add(word2);
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
			SyntacticNgram[] ngrams = ngramLine.getNgrams();			// Extract syntactic ngrams array for multiple uses
			total_count.set(ngramLine.getTotalCount());					// Extract total count integer for multiple uses

			for (int i = 0; i < ngrams.length; i++) 
			{
				SyntacticNgram currNgram = ngrams[i];
				System.err.println(currNgram.toString());
				String currWord = currNgram.getWord();
				String dependancyLabel = currNgram.getDependancyLabel();
				
				SyntacticNgram headIndexNgram = ngrams[currNgram.getHeadIndex() - 1];		// Get the 'head' word which points on current Ngram
				String headIndexWord = headIndexNgram.getWord(); 						// Get the head word from the index
				headIndexWord = Stemmer.stemWord(headIndexWord);						// Stem head-index word

				// If the head-index word is not a part of the gold standard dataset --> no need to build co-occurrences vector
				if (!(goldStandardWords.contains(headIndexWord))) {
					continue;
				}

				currWord = Stemmer.stemWord(currWord);									// Stem current word
				
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


		private PairWritable pair = new PairWritable();
		private Text value = new Text();
		private Text asterixFlag = new Text("*");


		// input: 		key: lexeme | feature | <lexeme, feature>, 					value: summed count
		// output: 		key: <lexeme, '*'> | <feature, '*'> | <lexeme, "1">, 		value: summed count | <feature, total count lexeme \w feature>
		//-----------------------------------------------------------> "1" value is not important, just a filler


		public void map(Text token, IntWritable total_count, Context context) throws IOException,  InterruptedException 
		{
			String key = token.toString();
			System.out.println(key);
			
			// Case token is of form: lexeme, word-dep_label --> split to make lexeme the key, and add feature to the value
			if (key.contains(",")) {
				String[] split = key.split(",");
				pair.setFirst(new Text(split[0]));								// create pair: <lexeme, "not-important">
				pair.setSecond(new Text("1"));									// Second in pair is not important
				value.set(split[1] + "," + total_count.toString());  			// create: "feature, count"
			}

			// Case token is lexeme only or feature only
			else {
				pair.setFirst(token); 											// create pair: <lexeme, '*'> or <feature, '*'>
				pair.setSecond(asterixFlag);
				value.set(total_count.toString());								// create value: total_count
			}

			context.write(pair, value);
		}
	}



	/*******************************************************************************************************/
	/********************************** 	Partitioner for MapReduce B	 **************************************/


	// Custom partitioner to ensure pairs of the same left element are partitioned to the same reducer


	public class PairWritablePartitioner extends Partitioner<PairWritable,Text> {

		@Override
		public int getPartition(PairWritable keyPair, Text value, int numPartitions) {
			return keyPair.getFirst().hashCode() % numPartitions;
		}
	}



	/*******************************************************************************************************/
	/******************************************** 	Reducer B	 *******************************************/



	public static class FeatureInfoReducer extends Reducer<PairWritable, Text, PairWritable, Text> {	


		private Text asterixFlag = new Text("*");
		private PairWritable pair = new PairWritable(null, new Text("1"));
		private String totalLexemeCount;
		private String lexeme;

		// input: 		key: <feature, '*'> | <lexeme, '*'> | <lexeme, "1">, 		value: Iterable<summed count> | Iterable<<feature, total count lexeme & feature>>
		// output: 		key: <feature, '*'> | feature, 		value: total count of feature | <lexeme, lexeme&feature count, lexeme's total count>


		public void reduce(PairWritable keyPair, Iterable<Text> countsOrFeatures, Context context) throws IOException, InterruptedException 
		{
			int sum = 0;

			// Case of: <something, '*'> --> '*' pair should be first in order
			if (keyPair.getSecond().equals(asterixFlag)) 			
			{
				// Case the input key is of a feature
				if (keyPair.getFirst().toString().contains("-")) {
					for (Text value : countsOrFeatures) {
						sum = Integer.parseInt(value.toString());
						System.err.println("reduceB asterix: " + value.toString());
						break;
					}
					context.write(keyPair, new Text(Integer.toString(sum)));	// Simply write as it is
				}

				// Case the input key is of a lexeme --> Get the total lexeme count for future uses
				else {
					for (Text value : countsOrFeatures) {
						totalLexemeCount = value.toString();
						break;
					}
				}

			}

			// case of: <lexeme, '1'>
			else {
				lexeme = keyPair.getFirst().toString();
				for (Text value : countsOrFeatures) 
				{
					System.err.println("reduceB NOT *: " + value.toString());
					String[] split = value.toString().split(",");					// split[0] = feature, split[1] = lexeme & feature count
					pair.setFirst(new Text(split[0])); 								// Set <feature, '1'>
					context.write(pair, new Text(lexeme + "," + split[1] + "," + totalLexemeCount));	// value: <lexeme, lexeme & feature count, lexeme's total count>
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

		private Text asterixFlag = new Text("*");
		private int totalFeatureCount = 0;
		private long totalLexemesCorpus = 0;
		private long totalFeaturesCorpus = 0;


		/*******************************************	Setup to get Global Counters	 ***********************************************/


		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			Cluster cluster = new Cluster(conf);
			Job currentJob = cluster.getJob(context.getJobID());
			totalLexemesCorpus = currentJob.getCounters().findCounter(GLOBAL_COUNTERS.NUM_OF_LEXEMES).getValue();		// Get overall counter of all lexemes
			totalFeaturesCorpus = currentJob.getCounters().findCounter(GLOBAL_COUNTERS.NUM_OF_FEATURES).getValue();		// Get overall counter of all features
		}


		/*******************************************	Reduce C	 ***********************************************/


		// input: 		key: <feature, '*'> | <feature, '1'>, 	value: Iterable<summed count> | Iterable<<lexeme, lexeme&feature count, lexeme's total count>>
		// output: 		key: feature, 							value: Feature data structure containing all computed measures


		public void reduce(PairWritable featureKey, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{

			// Case of: <feature, '*'> --> '*' pair should be first in order
			if (featureKey.getSecond().equals(asterixFlag)) 			
			{
				for (Text value : values) {
					totalFeatureCount = Integer.parseInt(value.toString());
					break;
				}
			}

			// Case of: <feature, '1'> --> containing in value <lexeme, lexeme&feature count, lexeme's total count>
			else {
				for (Text value : values) 
				{
					String[] split = value.toString().split(",");	// split[0] = lexeme, split[1] = lexeme&feature count, split[2] = lexeme's total count
					Feature feature = new Feature(featureKey.getFirst().toString(), split[0], totalFeatureCount, Integer.parseInt(split[2]));
					feature.computeAllMeasures(Integer.parseInt(split[1]), totalLexemesCorpus, totalFeaturesCorpus);
					System.err.println(feature.toString());
					context.write(featureKey.getFirst(), feature);			// output: feature_name, featureData
				}
			}
		}
	}
	
	
	// End of phase MapReduce C: 
	//			At this stage we hold all of the features as keys, and all of their data as values.
	//			Now, we want to compute the vectors similarities, as vectors are composed of the lexeme's features.
	//			Thus, we will want to be able to compare all coordinates of two vectors.
	
	
	
	/********************************************************************************************************************************************/

	
	
	
	/*******************************************************************************************************/
	/******************************************** 	Mapper D	 *******************************************/

	
	// We will want to be able to construct co-occurrences vectors.
	// Thus, will need to make each 'lexeme' as a key, and the reducer will get all of its features as values.
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
	/******************************************** 	Mapper D - Hidden	 *******************************************/

	
	// We will want to be able to compare two co-occurrences vectors.
	// Thus, every feature (= vector coordinate) that corresponds to both vectors will be send to the reducer as: <vector1 feature_i, vector2 feature_i>
	// For all features that exist only in one of the vectors, we will send <vector1 feature_j, NULL> or <NULL, vector2 feature_j>
	// For that, we will create and partition three kinds of keys:
	//							<lexeme1, lexeme2>, <lexeme1, '*'>, <'*', lexeme2>
	
	// We will need to create our custom partitioner for that cause, to make sure each key of the three forms will reach the same reducer.
	
	/*
	public static class IdentityMapperFeaturesData extends Mapper<Text, Feature, Text, Feature> {

		
		// input: 		key: feature_name, 			value: Feature data structure
		// output: 		key: feature_name, 			value: Feature data structure


		public void map(Text featureKey, Feature featureValue, Context context) throws IOException,  InterruptedException 
		{
			context.write(featureKey, featureValue);
		}
	}
	*/
	
	
	/*******************************************************************************************************/
	/******************************************** 	Reducer D 	 *******************************************/


	
	public static class VectorConstructorReducer extends Reducer<Text, Feature, Text, CooccurrencesVector> 
	{
		
		CooccurrencesVector vector = new CooccurrencesVector();
		
	
		/*******************************************	Reduce D	 ***********************************************/


		// input: 		key: lexeme_name, 	value: Iterable<Features>
		// output: 		key: lexeme_name, 	value: Lexeme's Co-occurrences vector


		public void reduce(Text lexemeName, Iterable<Feature> features, Context context) throws IOException, InterruptedException 
		{
			vector.resetVector();
			vector.setLexeme(lexemeName.toString());
			for (Feature feature : features) 
			{
				vector.addFeature(feature);
			}
			vector.computeVectorNorms();
			context.write(lexemeName, vector);
		}
	}
	
	
	
	/*******************************************************************************************************/
	/******************************************** 	Reducer D - Hidden	 *******************************************/


	/*
	public static class createCoordinatesReducer extends Reducer<Text, Feature, PairCoordinateWritable, PairMeasuresWritable> 
	{
		
		PairCoordinateWritable coordinatePair = new PairCoordinateWritable();
		PairMeasuresWritable measuresPair = new PairMeasuresWritable();
		MeasuresWritable null_measures = new MeasuresWritable();
		Feature currFeature = new Feature();
		Feature otherFeature = new Feature();
		String feature_name;
		
	
		/*******************************************	Reduce D	 ***********************************************/


		// input: 		key: feature_name, 	value: Iterable<Features>
		// output: 		key: <lexeme_i, lexeme_j, feature_name>, 	value: <Measures_i, Measures_j>
		//					 <lexeme_i, "*", feature_name>,			value: <Measures_i, null>
		//					 <"*", lexeme_j, feature_name>,			value: <null, Measures_j>

	/*
		public void reduce(Text featureName, Iterable<Feature> features, Context context) throws IOException, InterruptedException 
		{
			feature_name = featureName.toString();
			List<Feature> featuresList = new LinkedList<Feature>();
			for (Feature feature : features) 
			{
				featuresList.add(feature);
			}
			Feature[] featuresArray = featuresList.toArray(new Feature[featuresList.size()]);

			for (int i = 0; i < featuresArray.length; i++)
			{
				currFeature = featuresArray[i];
				
				// Write to context: 	<lexeme_i, "*", feature_name>, 		<measures_i, null_measures>
				coordinatePair.setPairCoordinateWritable(currFeature.getLexeme(), "*", feature_name);
				measuresPair.setPairMeasuresWritable(currFeature.getMeasures(), null_measures);
				context.write(coordinatePair, measuresPair);
				
				// Write to context: 	<"*", lexeme_i, feature_name>, 		<null_measures, measures_i>
				coordinatePair.setPairCoordinateWritable("*", currFeature.getLexeme(), feature_name);
				measuresPair.setPairMeasuresWritable(null_measures, currFeature.getMeasures());
				context.write(coordinatePair, measuresPair);
				
				
				// For every other corresponding lexeme coordinate:
				// Write to context:	<lexeme_i, lexeme_j, feature_name>, 	<measures_i, measures_j>
				
				for (int j = i + 1; j < featuresArray.length; j++)
				{
					otherFeature = featuresArray[j];
					coordinatePair.setPairCoordinateWritable(currFeature.getLexeme(), otherFeature.getLexeme(), feature_name);
					measuresPair.setPairMeasuresWritable(currFeature.getMeasures(), otherFeature.getMeasures());
					context.write(coordinatePair, measuresPair);
				}
			}
		}
	}
	
	
	
	
	/********************************************************************************************************************************************/

	
	
	/*******************************************************************************************************/
	/******************************************** 	Mapper E	 *******************************************/


	
	public static class FuzzyJoinMapper extends Mapper<Text, CooccurrencesVector, IntWritable, CooccurrencesVector> {
				
		
		private int fuzzy_join_num_buckets;
		private IntWritable indexedKey = new IntWritable();
		
		
		/*******************************************	Setup num of buckets to send each vector to	 ***********************************************/


		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			Cluster cluster = new Cluster(conf);
			Job currentJob = cluster.getJob(context.getJobID());
			fuzzy_join_num_buckets = (int) currentJob.getCounters().findCounter(GLOBAL_COUNTERS.NUM_OF_LEXEMES).getValue();		// Get overall counter of all lexemes
			if (fuzzy_join_num_buckets < 1) {
				fuzzy_join_num_buckets = 1;
			}
		}
		
		
		/*******************************************	Fuzzy Join Map	 ***********************************************/
		
		// reference to fuzzy join idea implementation: https://github.com/bkimmett/fuzzyjoin
		
		// input: 		key: lexeme_name, 	value: Co-occurrences vector
		// output: 		key: tag, 	value: Co-occurrences vector

		
		public void map(Text lexeme_name, CooccurrencesVector vector, Context context) throws IOException,  InterruptedException 
		{	
			
			// Output copies of vectors to all of the reducers
			for (int i = 0; i < fuzzy_join_num_buckets; i++)
			{ 	     
				context.write(indexedKey, vector); //write string and hashed numeric directional value
			}
		}
	}
	
	
	
	/*******************************************************************************************************/
	/************************* 	Partitioner for MapReduce E - Fuzzy Join	 ******************************/


	// Custom partitioner to ensure all Tagged keys vector go to all buckets


	public class FuzzyJoinPartitioner extends Partitioner<IntWritable, CooccurrencesVector> {

		@Override
		public int getPartition(IntWritable taggedKey, CooccurrencesVector vector, int numPartitions) {
			return taggedKey.get() % numPartitions;
		}
	}

	
	
	

	/*******************************************************************************************************/
	/******************************************** 	Mapper E - hidden	 *******************************************/

	// Passes data on to the reducer


	/*
	public static class VectorsSimilaritiesMapper extends Mapper<PairCoordinateWritable, PairMeasuresWritable, PairCoordinateWritable, PairMeasuresWritable> {

		// input: 		key: <lexeme_i, lexeme_j, feature_name>, 	value: <Measures_i, Measures_j>
		//					 <lexeme_i, "*", feature_name>,			value: <Measures_i, null>
		//					 <"*", lexeme_j, feature_name>,			value: <null, Measures_j>
		
		// output: 		key: <lexeme_i, lexeme_j, feature_name>, 	value: <Measures_i, Measures_j>
		//					 <lexeme_i, "*", feature_name>,			value: <Measures_i, null>
		//					 <"*", lexeme_j, feature_name>,			value: <null, Measures_j>

		
		public void map(PairCoordinateWritable key, PairMeasuresWritable value, Context context) throws IOException,  InterruptedException 
		{
			context.write(key, value);
		}
	}
	*/
	
	
	/*******************************************************************************************************/
	/*********************************** 	Reducer E - Fuzzy Join Reducer	 *******************************/
	
	
	
	public static class FuzzyJoinReducer extends Reducer<IntWritable, CooccurrencesVector, PairWritable, VectorsSimilaritiesWritable> {

		
		private PairVectorsWritable vectorsPair = new PairVectorsWritable();
		private CooccurrencesVector currVector = new CooccurrencesVector();
		private CooccurrencesVector otherVector = new CooccurrencesVector();



		/*******************************************	Reduce E	 ***********************************************/


		// input: 		key: 	tag, 	value: Iterable<CooccurrencesVector>		
		// output: 		key: 	feature, 							value: Feature data structure containing all computed measures

	
		public void reduce(IntWritable tag, Iterable<CooccurrencesVector> vectors, Context context) throws IOException, InterruptedException 
		{
			List<CooccurrencesVector> vectorsList = new LinkedList<CooccurrencesVector>();
			for (CooccurrencesVector vector : vectors)
			{
				vectorsList.add(vector);
			}
			CooccurrencesVector[] vectorsArray = vectorsList.toArray(new CooccurrencesVector[vectorsList.size()]);

			for (int i = 0; i < vectorsArray.length; i++)
			{
				currVector = vectorsArray[i];
				for (int j = i + 1; j < vectorsArray.length; j++)
				{
					otherVector = vectorsArray[j];
				}
			}
		}
	}
	

	
	
	/*******************************************************************************************************/
	/******************************************** 	Reducer E - Hidden	 *******************************************/
	
	/*
	
	public static class VectorsSimilaritiesReducer extends Reducer<PairCoordinateWritable, PairMeasuresWritable, PairWritable, VectorsSimilaritiesWritable> {

		private String asterixFlag = "*";
		private VectorsSimilaritiesWritable vectorsSimilarities = new VectorsSimilaritiesWritable();


		/*******************************************	Reduce E	 ***********************************************/


		// input: 		key: 	<lexeme_i, lexeme_j, feature_name>, 	value: <Measures_i, Measures_j>
		//					 	<lexeme_i, "*", feature_name>,			value: <Measures_i, null>
		//					 	<"*", lexeme_j, feature_name>,			value: <null, Measures_j>
		//				value:  Iterable< <Measures_i, Measures_j> | <Measures_i, null> | <null, Measures_j> >
		
		// output: 		key: feature, 							value: Feature data structure containing all computed measures

	/*
		public void reduce(PairCoordinateWritable lexemesFeaturePair, Iterable<PairMeasuresWritable> measuresPair, Context context) throws IOException, InterruptedException 
		{

			
		}
	}
	
	*/
	
	
	
	
	/*******************************************************************************************************/
	/******************************************** 	MAIN	 **********************************************/
	
	
	
	public static void main(String[] args) throws Exception 
	{
		
		if(args.length < 3) {
			System.err.println("Not enough arguments. needed <InputPath> <OutputPath> <GoldStandardDatasetFilePath>");
			System.exit(1);
		}
		
		
		Configuration conf = new Configuration();
		//conf.setInt("numOfSimilarTweets", Integer.parseInt(args[3]));
		//conf.set("numOfTweets","none");
		
		
		/***********************************	Job 1 - MapReduce A	 *****************************************/

		
		Job job1 = Job.getInstance(conf, "Tokens Count");
		job1.setJarByClass(CooccurrencesVectorsHadoop.class);
		
		
		/*	MapReduce A configurations	*/

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
		
		Log log = LogFactory.getLog(TokenizerMapper.class);

		if(!job1.waitForCompletion(true))
			System.exit(1);
		
		
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
		job3.setReducerClass(MeasuresOfAssocWithContextReducer.class);
		
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

		if(!job3.waitForCompletion(true))
			System.exit(1);
		
		
		/***********************************	Job 4 - MapReduce D	 *****************************************/

		/*
		Job job4 = Job.getInstance(conf, "calc similarity2");
		job4.setJarByClass(CooccurrencesVectorsHadoop.class);
		job4.setMapperClass(SimilarityMapper2.class);
		job4.setReducerClass(SimilarityReducer2.class);
		job4.setOutputKeyClass(TweetTweetDoubleWritableComparable.class);
		job4.setOutputValueClass(DoubleWritable.class);
		job4.setInputFormatClass(SequenceFileInputFormat.class);
		
		SequenceFileInputFormat.addInputPath(job4, new Path(args[1]+".intermediate3"));
		job4.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job4, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputPath(job4, new Path(args[1]+".intermediate4"));
		SequenceFileOutputFormat.setOutputCompressorClass(job4, GzipCodec.class);
		
		
		if(!job4.waitForCompletion(true))
			System.exit(1);
		*/
		
		/***********************************	Job 5 - MapReduce E	 *****************************************/
		
		/*
		Job job5 = Job.getInstance(conf, "calc similarity2");
		job5.setJarByClass(TweetSimHadoop.class);
		job5.setMapperClass(SimListMapper.class);
		job5.setReducerClass(SimListReducer.class);
		job5.setOutputKeyClass(Text.class);
		job5.setMapOutputKeyClass(TweetWritable.class);
		job5.setMapOutputValueClass(TweetDoubleWritable.class);
		job5.setOutputValueClass(Text.class);
		job5.setInputFormatClass(SequenceFileInputFormat.class);
		
		SequenceFileInputFormat.addInputPath(job5, new Path(args[1]+".intermediate4"));
		FileOutputFormat.setOutputPath(job5, new Path(args[1]));
		
		if(!job5.waitForCompletion(true))
			System.exit(1);
		
		*/
		Job[] jobs = { job1, job2, job3/*, job4, job5 */};
		for(int i = 0; i < 5; i++) {
			System.out.println("Phase " + (i+1) + " Key-Value Pairs:");
			System.out.println("\tPairs sent from Mapper: " + jobs[i].getCounters().findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue());
	        System.out.println("\tTotal size: " + jobs[i].getCounters().findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue() + " bytes");			
		}
	
	}
}











