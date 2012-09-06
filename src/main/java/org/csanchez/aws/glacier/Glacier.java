package org.csanchez.aws.glacier;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.CreateVaultRequest;
import com.amazonaws.services.glacier.model.CreateVaultResult;
import com.amazonaws.services.glacier.model.DeleteArchiveRequest;
import com.amazonaws.services.glacier.model.DeleteVaultRequest;
import com.amazonaws.services.glacier.model.DescribeVaultOutput;
import com.amazonaws.services.glacier.model.DescribeVaultRequest;
import com.amazonaws.services.glacier.model.DescribeVaultResult;
import com.amazonaws.services.glacier.model.GetJobOutputRequest;
import com.amazonaws.services.glacier.model.GetJobOutputResult;
import com.amazonaws.services.glacier.model.GlacierJobDescription;
import com.amazonaws.services.glacier.model.InitiateJobRequest;
import com.amazonaws.services.glacier.model.InitiateJobResult;
import com.amazonaws.services.glacier.model.JobParameters;
import com.amazonaws.services.glacier.model.ListJobsRequest;
import com.amazonaws.services.glacier.model.ListJobsResult;
import com.amazonaws.services.glacier.model.ListVaultsRequest;
import com.amazonaws.services.glacier.model.ListVaultsResult;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.UploadResult;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sns.model.UnsubscribeRequest;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;

/**
 * A command line client to Amazon Glacier based on AWS examples.
 * http://aws.amazon.com/glacier
 * 
 * Uses Glacier high level API for uploading, downloading, deleting files, and
 * the low-level one for retrieving vault inventory.
 * 
 * More info at http://docs.amazonwebservices.com/amazonglacier/latest/dev/
 * 
 * @author Carlos Sanchez <a href="mailto:carlos@apache.org">
 */
public class Glacier {

    private static long sleepTime = 600;
    private AmazonGlacierClient client;
    private AmazonSQSClient sqsClient;
    private AmazonSNSClient snsClient;

    private AWSCredentials credentials;
    private String region;
    
    private static boolean verbose = false;

    public Glacier(AWSCredentials credentials, String region) {
        this.credentials = credentials;
        this.region = region;
        client = new AmazonGlacierClient(credentials);
        client.setEndpoint("https://glacier." + region + ".amazonaws.com/");
    }

    public static void main(String[] args) throws Exception {

        Options options = commonOptions();
        if (args.length < 1) {
            printHelp(options);
            return;
        }

        String userHome = System.getProperty("user.home");
        File props = new File(userHome + "/AwsCredentials.properties");
        if (!props.exists()) {
            System.out.println("Missing " + props.getAbsolutePath());
            return;
        }

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);
        List<String> arguments = Arrays.asList(cmd.getArgs());

        AWSCredentials credentials = new PropertiesCredentials(new File(userHome + "/AwsCredentials.properties"));
        Glacier glacier = new Glacier(credentials, cmd.getOptionValue("region", "us-east-1"));

        if (arguments.size() == 0) {
            System.out.println("Missing action argument");
            return;        	
        }

        if (cmd.hasOption("verbose")) {
        	verbose = true;
        }
        
        if ("inventory".equals(arguments.get(0))) {
            glacier.inventory(arguments.get(1), cmd.getOptionValue("topic", "glacier"), cmd.getOptionValue("queue", "glacier"),
                    cmd.getOptionValue("file", "glacier.json"));
        } else if ("listvaults".equals(arguments.get(0))) {
            glacier.listVaults();       	
        } else if ("listjobs".equals(arguments.get(0))) {
            if (arguments.size() != 2) {
                printHelp(options);
                return;
            }
            glacier.listJobs(arguments.get(1));       	
        } else if ("describe".equals(arguments.get(0))) {
            if (arguments.size() != 2) {
                printHelp(options);
                return;
            }
            glacier.describe(arguments.get(1));       	
        } else if ("erase".equals(arguments.get(0))) {
            if (arguments.size() != 2) {
                printHelp(options);
                return;
            }
            glacier.erase(arguments.get(1));       	
        } else if ("create".equals(arguments.get(0))) {
            if (arguments.size() != 2) {
                printHelp(options);
                return;
            }
            glacier.create(arguments.get(1));       	
        } else if ("upload".equals(arguments.get(0))) {
            if (arguments.size() < 3) {
                printHelp(options);
                return;
            }
            for (String archive : arguments.subList(2, arguments.size())) {
                glacier.upload(arguments.get(1), archive);
            }
        } else if ("download".equals(arguments.get(0))) {
            if (arguments.size() != 4) {
                printHelp(options);
                return;
            }
            glacier.download(arguments.get(1), arguments.get(2), arguments.get(3));
        } else if ("delete".equals(arguments.get(0))) {
            if (arguments.size() != 3) {
                printHelp(options);
                return;
            }
            glacier.delete(arguments.get(1), arguments.get(2));
        } else {
            printHelp(options);
        }
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("glacier " + "upload vault_name file1 file2 ... | " + "download vault_name archiveId output_file | "
                + "delete vault_name archiveId | " + "inventory vault_name | " + "describe vault_name | " + "erase vault_name | "
        		+ "create vault_name | " + "listjobs vault_name (add -verbose to see \"Succeeded\" jobs) | " + "listvaults", options);
    }

    @SuppressWarnings("static-access")
    private static Options commonOptions() {
        Options options = new Options();
        Option region = OptionBuilder.withArgName("region").hasArg()
                .withDescription("Specify URL as the web service URL to use. Defaults to 'us-east-1'").create("region");
        options.addOption(region);

        Option topic = OptionBuilder.withArgName("topic_name").hasArg()
                .withDescription("SNS topic to use for inventory retrieval. Defaults to 'glacier'").create("topic");
        options.addOption(topic);

        Option queue = OptionBuilder.withArgName("queue_name").hasArg()
                .withDescription("SQS queue to use for inventory retrieval. Defaults to 'glacier'").create("queue");
        options.addOption(queue);

        Option output = OptionBuilder.withArgName("file_name").hasArg()
                .withDescription("File to save the inventory to. Defaults to 'glacier.json'").create("output");
        options.addOption(output);
        
        Option verbose = OptionBuilder.withArgName("verbose")
        		.withDescription("Verbose output (some actions only)").create("verbose");
        options.addOption(verbose);

        return options;
    }

    public void upload(String vaultName, String archive) {
        String msg = "Uploading " + archive + " to Glacier vault " + vaultName;
        System.out.println(msg);

        try {
            ArchiveTransferManager atm = new ArchiveTransferManager(client, credentials);
            UploadResult result = atm.upload(vaultName, archive, new File(archive));
            System.out.println("Uploaded " + archive + ": " + result.getArchiveId());
        } catch (Exception e) {
            throw new RuntimeException("Error " + msg, e);
        }
    }

    public void download(String vaultName, String archiveId, String downloadFilePath) {
        String msg = "Downloading " + archiveId + " from Glacier vault " + vaultName;
        System.out.println(msg);

        try {
            ArchiveTransferManager atm = new ArchiveTransferManager(client, credentials);
            atm.download(vaultName, archiveId, new File(downloadFilePath));
        } catch (Exception e) {
            throw new RuntimeException("Error " + msg, e);
        }
    }

    public void delete(String vaultName, String archiveId) {
        String msg = "Deleting " + archiveId + " from Glacier vault " + vaultName;
        System.out.println(msg);

        try {
            client.deleteArchive(new DeleteArchiveRequest().withVaultName(vaultName).withArchiveId(archiveId));
            System.out.println("Deleted archive: " + archiveId);
        } catch (Exception e) {
            throw new RuntimeException("Error " + msg, e);
        }
    }

    public void create(String vaultName) {
        String msg = "Requesting create Glacier vault " + vaultName + " (region " + this.region + ")";
        System.out.println(msg);
        try {
        	CreateVaultRequest request = new CreateVaultRequest().withVaultName(vaultName);
        	CreateVaultResult result = client.createVault(request);

        	System.out.println("Created vault successfully: " + result.getLocation());        
       	} catch (Exception e) {
       		System.err.println(e.getMessage());
        }    	
    }

    public void erase(String vaultName) {
        String msg = "Requesting delete Glacier vault " + vaultName + " (region " + this.region + ")";
        System.out.println(msg);
        try {
            DeleteVaultRequest request = new DeleteVaultRequest().withVaultName(vaultName);

            client.deleteVault(request);
            System.out.println("Deleted vault: " + vaultName);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }    	
    }

    public void describe(String vaultName) {
        String msg = "Requesting metadata from Glacier vault " + vaultName + " (region " + this.region + ")";
        System.out.println(msg);

        DescribeVaultRequest request = new DescribeVaultRequest().withVaultName(vaultName);
        DescribeVaultResult result = client.describeVault(request);
    
        System.out.print(
            "\nCreationDate: " + result.getCreationDate() +
            "\nLastInventoryDate: " + result.getLastInventoryDate() +
            "\nNumberOfArchives: " + result.getNumberOfArchives() + 
            "\nSizeInBytes: " + result.getSizeInBytes() + 
            "\nVaultARN: " + result.getVaultARN() + 
            "\nVaultName: " + result.getVaultName());
    }
    
    public void listJobs(String vault) {
    	String msg = "Requesting list of all jobs for vault " + vault + " (region " + this.region + ")";
    	System.out.println(msg);
    	
    	ListJobsRequest request = new ListJobsRequest(vault);
    	ListJobsResult result = client.listJobs(request);
    	
    	List<GlacierJobDescription> jobList = result.getJobList();
    	for (GlacierJobDescription descr : jobList) {
    		if (!verbose && descr.getStatusCode().equalsIgnoreCase("Succeeded")) {
    			continue;
    		}
    			
    		System.out.println("JobID: " + descr.getJobId()
    			+ "\nJobDescription: " + descr.getJobDescription()
    			+ "\nAction: " + descr.getAction()
    			+ "\nCreationDate: " + descr.getCreationDate()
    			+ "\nStatusCode: " + descr.getStatusCode()
    			+ "\n");
    	}
    }
    
    public void listVaults() {
    	String msg = "Requesting list of all vaults (region " + this.region + ")";
    	System.out.println(msg);
    	
    	ListVaultsRequest request = new ListVaultsRequest("-");
    	ListVaultsResult result = client.listVaults(request);
    	
    	List<DescribeVaultOutput> list = result.getVaultList();
    	for (DescribeVaultOutput output : list) {
    		System.out.println("ARN: " + output.getVaultARN() 
    			+ "\nName: " + output.getVaultName()
    			+ "\nCreationDate: " + output.getCreationDate()
    			+ "\nLastInventoryDate: " + output.getLastInventoryDate()
    			+ "\nNumberOfArchives: " + output.getNumberOfArchives()
    			+ "\nSizeInBytes: " + output.getSizeInBytes()
    			+ "\n");
    	}
    }

    public void inventory(String vaultName, String snsTopicName, String sqsQueueName, String fileName) {
        String msg = "Requesting inventory from Glacier vault " + vaultName + ". It might take around 4 hours.";
        System.out.println(msg);

        sqsClient = new AmazonSQSClient(credentials);
        sqsClient.setEndpoint("https://sqs." + region + ".amazonaws.com");
        snsClient = new AmazonSNSClient(credentials);
        snsClient.setEndpoint("https://sns." + region + ".amazonaws.com");

        try {
            QueueConfig config = setupSQS(sqsQueueName);

            config = setupSNS(config, snsTopicName);

            String jobId = initiateJobRequest(vaultName, config.snsTopicARN);
            System.out.println("Jobid = " + jobId);

            Boolean success = waitForJobToComplete(jobId, config.sqsQueueURL);
            if (!success) {
                throw new Exception("Job did not complete successfully.");
            }

            downloadJobOutput(vaultName, jobId, fileName);

            cleanUp(config);

        } catch (Exception e) {
            System.err.println("Archive retrieval failed.");
            System.err.println(e);
        }
    }

    private QueueConfig setupSQS(String sqsQueueName) {
        QueueConfig config = new QueueConfig();
        CreateQueueRequest request = new CreateQueueRequest().withQueueName(sqsQueueName);
        CreateQueueResult result = sqsClient.createQueue(request);
        config.sqsQueueURL = result.getQueueUrl();

        GetQueueAttributesRequest qRequest = new GetQueueAttributesRequest().withQueueUrl(config.sqsQueueURL)
                .withAttributeNames("QueueArn");

        GetQueueAttributesResult qResult = sqsClient.getQueueAttributes(qRequest);
        config.sqsQueueARN = qResult.getAttributes().get("QueueArn");

        Policy sqsPolicy = new Policy().withStatements(new Statement(Effect.Allow).withPrincipals(Principal.AllUsers)
                .withActions(SQSActions.SendMessage).withResources(new Resource(config.sqsQueueARN)));
        Map<String, String> queueAttributes = new HashMap<String, String>();
        queueAttributes.put("Policy", sqsPolicy.toJson());
        sqsClient.setQueueAttributes(new SetQueueAttributesRequest(config.sqsQueueURL, queueAttributes));
        return config;
    }

    private QueueConfig setupSNS(QueueConfig config, String snsTopicName) {
        CreateTopicRequest request = new CreateTopicRequest().withName(snsTopicName);
        CreateTopicResult result = snsClient.createTopic(request);
        config.snsTopicARN = result.getTopicArn();

        SubscribeRequest request2 = new SubscribeRequest().withTopicArn(config.snsTopicARN).withEndpoint(config.sqsQueueARN)
                .withProtocol("sqs");
        SubscribeResult result2 = snsClient.subscribe(request2);

        config.snsSubscriptionARN = result2.getSubscriptionArn();
        return config;
    }

    private String initiateJobRequest(String vaultName, String snsTopicARN) {

        JobParameters jobParameters = new JobParameters().withType("inventory-retrieval").withSNSTopic(snsTopicARN);

        InitiateJobRequest request = new InitiateJobRequest().withVaultName(vaultName).withJobParameters(jobParameters);

        InitiateJobResult response = client.initiateJob(request);

        return response.getJobId();
    }

    private Boolean waitForJobToComplete(String jobId, String sqsQueueUrl) throws InterruptedException, JsonParseException, IOException {

        Boolean messageFound = false;
        Boolean jobSuccessful = false;
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getJsonFactory();
        Date start = new Date();

        while (!messageFound) {
            long minutes = (new Date().getTime() - start.getTime()) / 1000 / 60;
            System.out.println("Checking for messages: " + minutes + " mins. elapsed");
            List<Message> msgs = sqsClient.receiveMessage(new ReceiveMessageRequest(sqsQueueUrl).withMaxNumberOfMessages(10)).getMessages();

            if (msgs.size() > 0) {
                for (Message m : msgs) {
                    JsonParser jpMessage = factory.createJsonParser(m.getBody());
                    JsonNode jobMessageNode = mapper.readTree(jpMessage);
                    String jobMessage = jobMessageNode.get("Message").getTextValue();

                    JsonParser jpDesc = factory.createJsonParser(jobMessage);
                    JsonNode jobDescNode = mapper.readTree(jpDesc);
                    String retrievedJobId = jobDescNode.get("JobId").getTextValue();
                    String statusCode = jobDescNode.get("StatusCode").getTextValue();

                    if (retrievedJobId.equals(jobId)) {
                        messageFound = true;
                        if (statusCode.equals("Succeeded")) {
                            jobSuccessful = true;
                        }
                    } else if (verbose) {
                        if (!jobMessage.isEmpty()) {
                            String action = jobDescNode.get("Action").getTextValue();
                            String completionDate = jobDescNode.get("CompletionDate").getTextValue();
                            String creationDate = jobDescNode.get("CreationDate").getTextValue();
                        	// System.out.println("\njobMessage: " + jobMessage);
                        	System.out.println("JobId: " + retrievedJobId);
                        	System.out.println("Action: " + action);
                        	System.out.println("StatusCode: " + statusCode);
                        	System.out.println("CompletionDate: " + completionDate);
                        	System.out.println("CreationDate: " + creationDate + "\n");
                        }                    	
                    }
                }

            } else {
                Thread.sleep(sleepTime * 1000);
            }
        }
        return (messageFound && jobSuccessful);
    }

    private void downloadJobOutput(String vaultName, String jobId, String fileName) throws IOException {

        GetJobOutputRequest getJobOutputRequest = new GetJobOutputRequest().withVaultName(vaultName).withJobId(jobId);
        GetJobOutputResult getJobOutputResult = client.getJobOutput(getJobOutputRequest);

        FileWriter fstream = new FileWriter(fileName);
        BufferedWriter out = new BufferedWriter(fstream);
        BufferedReader in = new BufferedReader(new InputStreamReader(getJobOutputResult.getBody()));
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            out.write(inputLine);
        }
        out.close();
        System.out.println("Retrieved inventory to " + fileName);
    }

    private void cleanUp(QueueConfig config) {
        snsClient.unsubscribe(new UnsubscribeRequest(config.snsSubscriptionARN));
        snsClient.deleteTopic(new DeleteTopicRequest(config.snsTopicARN));
        sqsClient.deleteQueue(new DeleteQueueRequest(config.sqsQueueURL));
    }

    class QueueConfig {
        String sqsQueueURL, sqsQueueARN, snsTopicARN, snsSubscriptionARN;
    }
}
