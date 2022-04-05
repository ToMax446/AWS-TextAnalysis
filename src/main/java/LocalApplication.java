import com.amazonaws.services.connect.model.CreateQueueResult;
import com.amazonaws.services.ec2.model.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.lang.System.exit;


public class LocalApplication {

    static String inputFileName;
    static String outputFileName;
    static String n; // workers’ files ratio (max files per wor

    public static boolean isManagerActive(Ec2Client ec2) {
        boolean done = false;
        DescribeInstancesResponse response = ec2.describeInstances();
        outerLoop:
        // a label for break
        for (Reservation reservation : response.reservations()) { // every response has reservations
            for (Instance instance : reservation.instances()) { // every reservation gas instances
                for (Tag tag : instance.tags()) { // every instance has tags
                    if (tag.value().equals("manager") && instance.state().name().toString().equals("running")) {
                        done = true;
                        break outerLoop;
                    }
                }
            }
        }
        return done;
    }

    public static String createNewManager(Ec2Client ec2) {
        String amiId = "ami-076515f20540e6e0b";
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T1_MICRO)
                .maxCount(1)
                .minCount(1)
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("Name")
                .value("manager")
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "Successfully started EC2 Instance %s based on AMI %s",
                    instanceId, amiId);
            return instanceId;

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            exit(1);
        }

        return "";
    }

    public static void main(String[] args) {
//        Ec2Client ec2 = Ec2Client.create(); // one shared object
        String uniqueBucket = UUID.randomUUID().toString();
        Ec2Client ec2 = Ec2Client.builder()
                .region(Region.US_WEST_2)
                .build();
        String managerUrl;
        try {
            inputFileName = args[0];
            File f = new File(inputFileName);
            if (!f.exists()) {
                System.out.println("Input file does not exist. Please load the app again.");
                exit(1);
            }
            outputFileName = args[1];
            n = args[2];
        } catch (Exception e) {
            System.out.println("can't read arguments as expected.\ncheck your arguments and load the app again.");
            exit(1);
        }
        if (!isManagerActive(ec2))
            managerUrl = createNewManager(ec2);
        S3Client s3 = S3Client.builder().region(Region.US_WEST_2).build();
        s3.createBucket(CreateBucketRequest.builder().bucket(uniqueBucket).build());
        PutObjectResponse file = s3.putObject(PutObjectRequest.builder().bucket(uniqueBucket).key("ass1").build(), RequestBody.fromFile(new File(inputFileName)));

        SqsClient sqs = SqsClient.builder().region(Region.US_WEST_2).build();
        CreateQueueResponse localQueueRes =
                sqs.createQueue(CreateQueueRequest.builder().queueName("dsp-local-to-manager-queue").build());
        String localManagerQueueUrl = localQueueRes.queueUrl(); // a url to a queue from the local app to the manager

        CreateQueueResponse managerQueueRes =
                sqs.createQueue(CreateQueueRequest.builder().queueName("dsp-manager-to-local-queue").build());
        String managerLocalQueueUrl = managerQueueRes.queueUrl(); // a url to a queue from the manager to the local app
        sqs.sendMessage(SendMessageRequest.builder().queueUrl(localManagerQueueUrl).messageBody("input file__"+uniqueBucket+"__"+ file).build()); //CHECK

        for (;;)    // loop until we get a message
        {
            List<Message> messages =
                    sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(managerLocalQueueUrl).build()).messages();
            if (!messages.isEmpty()) {
                String[] message = messages.get(0).body().split("__");
                if (message[0].equals("done task")) {
                    InputStream sum = s3.getObject(GetObjectRequest.builder().bucket(uniqueBucket).key(message[1]).build());
                    String text = new BufferedReader(
                            new InputStreamReader(sum, StandardCharsets.UTF_8)).lines()
                            .collect(Collectors.joining("\n"));

                    try { // create html file
                        FileWriter fd = new FileWriter(outputFileName + ".html");
                        fd.write(text);
                        fd.close();
                        if (args.length > 3 && args[3].equals("terminate")) //if we are on termination mode send a termination message to the manager
                            sqs.sendMessage(SendMessageRequest.builder().queueUrl(localManagerQueueUrl).messageBody("terminate").build());
                        break; //break after
                    } catch (IOException e) {
                        System.out.println("An error occurred.");
                        exit(1);
                    }
                }
            }
       }
    }
}