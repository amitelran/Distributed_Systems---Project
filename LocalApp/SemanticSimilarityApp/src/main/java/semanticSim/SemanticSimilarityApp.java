package semanticSim;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.Cluster;
import com.amazonaws.services.elasticmapreduce.model.ClusterState;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListStepsResult;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class SemanticSimilarityApp {

	public static String OUTPUTFILENAME = "output.txt";
	public static String BUCKET_NAME = "yoav.amit.dsp-project";

	public static S3Object downloadFile(AmazonS3 s3, String fileBucket, String fileKey){
		System.out.println("Downloading output file \"" + fileKey + "\"");
		S3Object object = s3.getObject(new GetObjectRequest(fileBucket, fileKey));
		return object;
	}

	public static void main(String[] args) throws Exception { 	
		int numOfFiles; 
		String clusterId = null;
		String stepId = null;
		boolean hasIds = false;
		
		if(args.length < 1){
			System.out.println("Please provide <num of Google Syntactic Ngrams to process>.");
			System.exit(1);
		}
		try{
			numOfFiles = Integer.parseInt(args[0]);
		}
		catch(NumberFormatException e){
			System.out.println(args[0] + " is not a valid number.");
			return;
		}

		// set credentials
		System.out.print("Welcome to Semantic Similarity calculator!\n\nSetting AWS credentials... ");

		AWSCredentialsProvider credentials = new DefaultAWSCredentialsProviderChain();
		try{
			credentials.getCredentials();      	
		}
		catch(SdkClientException e){
			Scanner scanner = new Scanner(System.in);
			System.out.print("Enter your credentials\nAccess Key: ");
			String accessKey = scanner.nextLine();
			System.out.print("Secret Key: ");
			String secretKey = scanner.nextLine(); 
			scanner.close();

			AWSCredentials staticCredentials= new BasicAWSCredentials(accessKey,secretKey);
			credentials = new AWSStaticCredentialsProvider(staticCredentials);
		}
		
		System.out.println("done");

		try{
			System.out.print("Configuring EMR cluster... ");
			AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard().withCredentials(credentials)
					.withRegion(Regions.US_EAST_1).build();
			System.out.println("done");

			System.out.print("Configuring Hadoop step... ");
			HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
					.withJar("s3n://"+BUCKET_NAME+"/SemanticSimilarity.jar")
					.withArgs("s3n://"+BUCKET_NAME+"/input",
							"s3n://"+BUCKET_NAME+"/output","s3n://"+BUCKET_NAME+"/word-relatedness.txt",Integer.toString(numOfFiles));

			StepConfig stepConfig = new StepConfig()
					.withName("semantic similarity program")
					.withHadoopJarStep(hadoopJarStep)
					.withActionOnFailure("TERMINATE_JOB_FLOW");

			JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
					.withMasterInstanceType(InstanceType.M32xlarge.toString())
					.withSlaveInstanceType(InstanceType.M32xlarge.toString())
					.withInstanceCount(5)
					.withHadoopVersion("2.7.3")
					.withKeepJobFlowAliveWhenNoSteps(false)
					.withPlacement(new PlacementType("us-east-1a"));

			RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
					.withServiceRole("EMR_Defaul"
							+ "tRole")
					.withJobFlowRole("EMR_EC2_DefaultRole")
					.withName("Semantic Similarity")
					.withInstances(instances)
					.withSteps(stepConfig)
					.withReleaseLabel("emr-5.6.0")
					.withLogUri("s3n://"+BUCKET_NAME+"/logs/");

			runFlowRequest.setServiceRole("EMR_DefaultRole");
			System.out.println("done");

			System.out.print("Starting EMR job... ");
			RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
			String jobFlowId = runJobFlowResult.getJobFlowId();
			System.out.println("done");
			System.out.println("Started job flow with id: " + jobFlowId + " , Please wait...");

			while(true) {
				DescribeClusterRequest desc = new DescribeClusterRequest()
						.withClusterId(runJobFlowResult.getJobFlowId());
				DescribeClusterResult clusterResult = mapReduce.describeCluster(desc);
				Cluster cluster = clusterResult.getCluster();
				
				if(!hasIds){
					clusterId = cluster.getId();
					
					ListStepsRequest liststeps = new ListStepsRequest()
							.withClusterId(runJobFlowResult.getJobFlowId());
					ListStepsResult stepsResult = mapReduce.listSteps(liststeps);
					
					stepId = stepsResult.getSteps().get(0).getId();
					
					hasIds = true;
				}
				
				String status = cluster.getStatus().getState();
				System.out.printf(new SimpleDateFormat("HH:mm:ss").format(new Date()) + " Status: %s\n", status);
				if(status.equals(ClusterState.TERMINATED.toString())) {
					break;
				}
				else if(status.equals(ClusterState.TERMINATED_WITH_ERRORS.toString())){
					System.out.println(cluster.getStatus().getStateChangeReason().getMessage());
					System.out.println("Please check logs on s3");
					System.exit(1);
				}
				try {
					TimeUnit.SECONDS.sleep(30);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}


			AmazonS3 s3 = AmazonS3ClientBuilder.standard().withCredentials(credentials).withRegion(Regions.US_EAST_1).build();

			ObjectListing list = s3.listObjects(new ListObjectsRequest().withBucketName(BUCKET_NAME).withPrefix("output/part"));

			FileWriter writer = new FileWriter(OUTPUTFILENAME);
			BufferedWriter out = new BufferedWriter(writer);
			BufferedReader reader;
			char[] cbuf = new char[1024];
			for(S3ObjectSummary elem : list.getObjectSummaries()){
				S3Object outputFileObj = downloadFile(s3, elem.getBucketName(), elem.getKey()); 

				reader = new BufferedReader(new InputStreamReader(outputFileObj.getObjectContent()));		// Read content of input file object

				System.out.println("Appending \"" + elem.getKey().substring(7) + "\" to \"" + OUTPUTFILENAME + "\"");
				int b = 0;
				while ( (b = reader.read(cbuf)) >= 0) {
					out.write(cbuf, 0, b);
					out.flush();
				}
				s3.deleteObject(new DeleteObjectRequest(elem.getBucketName(), elem.getKey()));
			}
			out.close();

			System.out.println("Job done successfully. see file \"" + OUTPUTFILENAME + "\"");
			
			S3Object stdoutLog = s3.getObject(BUCKET_NAME, "logs/" + clusterId + "/steps/" + stepId + "/stdout.gz");
			
			InputStream gzipStream = new GZIPInputStream(stdoutLog.getObjectContent());
			Reader decoder = new InputStreamReader(gzipStream, "UTF-8");

			reader = new BufferedReader(decoder);
			String line;
			System.out.println("\nKey-Value Stats\n======================");
			while ((line = reader.readLine()) != null) {
				System.out.println(line);
			}
			System.out.println("======================");
			
			list = s3.listObjects(new ListObjectsRequest().withBucketName(BUCKET_NAME).withPrefix("output"));

			System.out.println("Deleting intermediate output files...");

			for(S3ObjectSummary elem : list.getObjectSummaries()){            
				s3.deleteObject(new DeleteObjectRequest(elem.getBucketName(), elem.getKey()));
			}
			
			System.out.print("We're done here, goodbye!");
		}
		catch (AmazonServiceException ase) {
			ExcpetionPrint.printErrorASE(ase);
		} catch (AmazonClientException ace) {
			ExcpetionPrint.printErrorACE(ace);
		}
	}
}
