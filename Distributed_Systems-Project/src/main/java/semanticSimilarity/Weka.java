package semanticSimilarity;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

import weka.classifiers.Evaluation;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;



public class Weka {


	
	/***************	 Create ARFF file for WEKA	 ***************/


	public static int create_ARFF_File(String path) 
	{
		int ret;
		try {
			PrintWriter writer = new PrintWriter(path, "UTF-8");
			create_ARFF_Header(writer);								// Create ARFF header for file
			writer.close();
			ret = 1;
		} 
		catch (IOException e) {
			System.err.println("Error occured while creating Weka file\n");
			ret = 0;
		}
		return ret;
	}



	/***************	 Add ARFF header for created file  ***************/


	public static void create_ARFF_Header(PrintWriter writer)
	{
		writer.println("% Distributed Systems Programming: Project");
		writer.println("% Creators: Yoav Beeri, Amit Elran");
		writer.println();		

		writer.println("@RELATION biarcs");
		writer.println();

		//writer.println("@ATTRIBUTE word1	STRING");
		//writer.println("@ATTRIBUTE word2	STRING");

		/* Raw Frequency Measures */
		writer.println("@ATTRIBUTE raw_freq_manhattan	NUMERIC");
		writer.println("@ATTRIBUTE raw_freq_euclidean	NUMERIC");
		writer.println("@ATTRIBUTE raw_freq_cosine	NUMERIC");
		writer.println("@ATTRIBUTE raw_freq_jaccard	NUMERIC");
		writer.println("@ATTRIBUTE raw_freq_dice	NUMERIC");

		/* Relative Frequency Measures */
		writer.println("@ATTRIBUTE rel_freq_manhattan	NUMERIC");
		writer.println("@ATTRIBUTE rel_freq_euclidean	NUMERIC");
		writer.println("@ATTRIBUTE rel_freq_cosine	NUMERIC");
		writer.println("@ATTRIBUTE rel_freq_jaccard	NUMERIC");
		writer.println("@ATTRIBUTE rel_freq_dice	NUMERIC");

		/* PMI Measures */
		writer.println("@ATTRIBUTE pmi_manhattan	NUMERIC");
		writer.println("@ATTRIBUTE pmi_euclidean	NUMERIC");
		writer.println("@ATTRIBUTE pmi_cosine	NUMERIC");
		writer.println("@ATTRIBUTE pmi_jaccard	NUMERIC");
		writer.println("@ATTRIBUTE pmi_dice	NUMERIC");

		/* T-test Measures */
		writer.println("@ATTRIBUTE t_test_manhattan	NUMERIC");
		writer.println("@ATTRIBUTE t_test_euclidean	NUMERIC");
		writer.println("@ATTRIBUTE t_test_cosine	NUMERIC");
		writer.println("@ATTRIBUTE t_test_jaccard	NUMERIC");
		writer.println("@ATTRIBUTE t_test_dice	NUMERIC");

		writer.println("@ATTRIBUTE similar	{false,true}");

		writer.println();
		writer.println("@DATA");
	}




	/***************	 Read S3 output file data, and append  to ARFF file	 ***************/


	public static int write_S3File_to_ARFF_File(BufferedReader reader, String arffFile) 
	{
		int ret = 0;
		try {
			PrintWriter writer = new PrintWriter(new FileWriter(arffFile, true));			/* Append to file = true */
			try 
			{
				/* Read line by line from S3 file, but put aside the lexemes pair (strings), and use them after the classification */

				String line;
				while((line = reader.readLine()) != null) 
				{
					String lexemes = "% ";
					String measures = "";
					int i = 0;
					while (i < line.length() && !Character.isDigit(line.charAt(i))) {
						lexemes += line.charAt(i);
						i++;
					}
					writer.println(lexemes);
					int j = i;
					while (j < line.length()) {
						measures += line.charAt(j);
						j++;
					}
					writer.println(measures);
				}
			}
			finally {
				writer.close();
				ret = 1;
			}
		} 
		catch (IOException e) {
			System.err.println("Error occured in append_data_ARFF_File\n");
			ret = 0;
		}
		return ret;
	}



	/***************	 Perform WEKA evaluation	  ***************/



	public static int evaluateWEKA(String path, String outputPath) throws Exception 
	{
		BufferedReader buffReader = null;
		int folds = 10;
		int ret = 0;
		try 
		{
			buffReader = new BufferedReader(new FileReader(path));		// Read the vectors similarities file
			Instances data = new Instances(buffReader);					// Instances will be the data lines from the file
			if (data.numInstances() < folds) 
			{
				folds = data.numInstances();							// Cannot have more folds than instances
			}
			data.setClassIndex(data.numAttributes() - 1);				// This means we are setting the 'similar' attribute (placed last in attributes)

			buffReader.close();			// Finished with buffered reader

			RandomForest randForest = new RandomForest();						// Set Random-Forest classifier (classifier doesn't matter)
			Evaluation eval = new Evaluation(data);								// We are setting  the data file to the Evaluation
			eval.crossValidateModel(randForest, data, folds, new Random(1));	// <classifier, data file, , 10-fold cross-validation, random number generator for randomization>

			System.out.println(eval.toSummaryString(("\nWeka Results\n===========\n"), true));
			System.out.println(eval.fMeasure(1) + " " + eval.precision(1) + " " + eval.recall(1));		// F1 measure, precision, recall stats ('0' is the index of the class to consider as "positive")
			System.out.println(eval.toMatrixString());
			
			ret = writeResultsToFile(outputPath, eval);					// Write WEKA classification results to file
		}

		catch (FileNotFoundException e1) 
		{
			System.err.println("Error occured in evaluateWEKA\n");
			ret = 0;
		}
		return ret;
	}




	/***************	 Write WEKA results to output file	 ***************/



	public static int writeResultsToFile(String outputPath, Evaluation eval) throws Exception 
	{
		int ret = 0;
		try {
			PrintWriter writer = new PrintWriter(outputPath, "UTF-8");
			writer.println("Yoav Beeri & Amit Elran - Distributed Systems Programming Project");
			writer.println((eval.toSummaryString(("\nWeka Results\n========================\n"), true)));
			writer.println("F1 Measure: " + eval.fMeasure(1));
			writer.println("Precision: " + eval.precision(1));
			writer.println("Recall: " + eval.recall(1));
			writer.println();
			writer.println("True-Positive rate: " + eval.truePositiveRate(1));
			writer.println("False-Positive rate: " + eval.falsePositiveRate(1));
			writer.println("True-Negative rate: " + eval.trueNegativeRate(1));
			writer.println("False-Negative rate: " + eval.falseNegativeRate(1));
			writer.println();
			writer.println(eval.toMatrixString());
			writer.println("========================");
			writer.close();
			ret = 1;
		} 
		catch (IOException e) {
			System.err.println("Error occured while writing Weka results to output file\n");
			ret = 0;
		}
		return ret;
	}

}
