package semanticSimilarity;


import java.io.File;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3Object;



public class S3 {

	private AmazonS3 s3;

	
	public S3(AWSCredentialsProvider credentials){
		s3 = AmazonS3ClientBuilder.standard().withCredentials(credentials).withRegion(Regions.US_EAST_1).build();
	}

	
	
	/*********** 	Upload file to S3 bucket	 ***********/
	
	
	public String uploadFile(String bucketName, String filepath) {
		String key = null;
		try {
			/*
			 * Create a new S3 bucket - Amazon S3 bucket names are globally unique,
			 * so once a bucket name has been taken by any user, you can't create
			 * another bucket with that same name.
			 *
			 * You can optionally specify a location for your bucket if you want to
			 * keep your data closer to your applications or users.
			 */
			
			System.out.println("Creating bucket " + bucketName + "\n");
			s3.createBucket(bucketName);

			/*
			 * List the buckets in your account
			 */
			System.out.println("Listing buckets");
			for (Bucket bucket : s3.listBuckets()) {
				System.out.println(" - " + bucket.getName());
			}
			System.out.println();

			/*
			 * Upload an object to your bucket - You can easily upload a file to
			 * S3, or upload directly an InputStream if you know the length of
			 * the data in the stream. You can also specify your own metadata
			 * when uploading to S3, which allows you set a variety of options
			 * like content-type and content-encoding, plus additional metadata
			 * specific to your applications.
			 */
			System.out.println("Uploading a new object to S3 from a file: " + filepath + "\n");
			File file = new File(filepath);

			key = file.getName().replace('\\', '_').replace('/','_').replace(':', '_') + "_" + UUID.randomUUID();
			PutObjectRequest req = new PutObjectRequest(bucketName, key, file);
			s3.putObject(req);
			return key;
		}
		catch (AmazonServiceException ase) {
			ExcpetionPrint.printErrorASE(ase);
		} catch (AmazonClientException ace) {
			ExcpetionPrint.printErrorACE(ace);
		}
		return null;
	}

	
	
	/*********** 	Download file from S3 bucket	 ***********/

	
	/*
	 * Download an object - When you download an object, you get all of
	 * the object's metadata and a stream from which to read the contents.
	 * It's important to read the contents of the stream as quickly as
	 * possibly since the data is streamed directly from Amazon S3 and your
	 * network connection will remain open until you read all the data or
	 * close the input stream.
	 *
	 * GetObjectRequest also supports several other options, including
	 * conditional downloading of objects based on modification times,
	 * ETags, and selectively downloading a range of an object.
	 */
	

	public S3Object downloadFile(String fileBucket, String fileKey)
	{
		System.out.println("Downloading output file \"" + fileKey + "\"");
		S3Object object = s3.getObject(new GetObjectRequest(fileBucket, fileKey));
		return object;
	}
	
	
	public static S3Object downloadFile(AmazonS3 s3, String fileBucket, String fileKey){
		System.out.println("Downloading output file \"" + fileKey + "\"");
		S3Object object = s3.getObject(new GetObjectRequest(fileBucket, fileKey));
		return object;
	}

	
}

