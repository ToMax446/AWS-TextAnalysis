import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import static java.lang.System.exit;

import tools.AWSAbstractions;
import tools.AWSAbstractions.*;

public class LocalApplication {

    // ----------
    // FIELDS
    public static String input_file_name;
    public static String output_file_name;
    public static String n;
    // ----------

    public static boolean is_Manager_Active(Ec2Client ec2) {
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

    public static List<Bucket> initialized_Buckets(S3Client s3) {
        List<Bucket> buckets = s3.listBuckets().buckets();
        if (buckets.isEmpty()) {
            s3.createBucket(CreateBucketRequest.builder().bucket("input").build());
            s3.createBucket(CreateBucketRequest.builder().bucket("output").build());
            s3.createBucket(CreateBucketRequest.builder().bucket("summary").build());
            s3.createBucket(CreateBucketRequest.builder().bucket("parser").build());
            s3.putObject(PutObjectRequest.builder().bucket("input").key("model").build(), RequestBody.fromFile(new File("src/main/resources/englishPCFG.ser.gz")));
            return s3.listBuckets().buckets();
        }
        return buckets;
    }

    public static void main(String[] args) {
        Region region = Region.US_EAST_1; // set region
        Ec2Client ec2 = Ec2Client.builder().credentialsProvider(ProfileCredentialsProvider.create()).region(region).build();
        S3Client s3 = S3Client.create(); // connect to S3 service
        SqsClient sqs = SqsClient.create(); // connect to SQS service
        String instance_ID = null;
        String local_application_id = UUID.randomUUID().toString();
        input_file_name = args[0];
        File input_file = new File(input_file_name);
        if (!input_file.exists()) {
            System.out.println("Input file does not exist. Please load the app again.");
            exit(1);
        }
        output_file_name = args[1];
        n = args[2];
        // ----------
        // need to fix!
        if (!is_Manager_Active(ec2)) {
            String user_data =  "#! /bin/bash\n"+"wget https://"+/*PLACEHOLDER*/+"/Manager.jar\n"+"java -jar Manager.jar\n";
            instance_ID = AWSAbstractions.create_EC2_Instance(ec2, "manager", "ami-076515f20540e6e0b", user_data);
        }
        // ----------
        initialized_Buckets(s3);
        PutObjectResponse input_file_in_S3 = s3.putObject(PutObjectRequest.builder().bucket("input").key(local_application_id).build(), RequestBody.fromFile(input_file));
        String local_to_manager_url = AWSAbstractions.queue_Setup(sqs, "dsp-local-to-manager-queue");
        String manager_to_local_url = AWSAbstractions.queue_Setup(sqs, "dsp-manager-to-local-queue");
        sqs.sendMessage(SendMessageRequest.builder().queueUrl(local_to_manager_url).messageBody(n+"\t"+local_application_id+"\t"+input_file).build());

        if (args.length > 3 && args[3].equals("terminate")) //if we are on termination mode send a termination message to the manager
            sqs.sendMessage(SendMessageRequest.builder().queueUrl(local_to_manager_url).messageBody("terminate").build());
        for (;;) // wait until we get a "done" msg
        {
            List<Message> messages =
                    sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(manager_to_local_url).build()).messages();
            if (!messages.isEmpty()) {
                String[] message = messages.get(0).body().split("\t");
                if (message[0].equals("done task")) {
                    InputStream sum = s3.getObject(GetObjectRequest.builder().bucket("summary").key(local_application_id).build());
                    String text = new BufferedReader(
                            new InputStreamReader(sum, StandardCharsets.UTF_8)).lines()
                            .collect(Collectors.joining("\n"));
                    try { // create html file
                        FileWriter fd = new FileWriter(output_file_name + ".html");
                        fd.write(text);
                        fd.close();
                        break; // break after
                    } catch (IOException e) {
                        System.out.println("An error occurred.");
                        exit(1);
                    }
                }
            }
       }
    }
}