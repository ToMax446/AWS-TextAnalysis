import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;

import java.io.InputStream;

import static java.lang.System.exit;

public class Manager {
    static int number_of_workers = 0;
    static int worker_per_message = 0;
    static int max_number_of_workers = 18;

    public String create_Worker(Ec2Client ec2) {
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
                .value("worker")
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

    private void bootstraps_Nodes(Ec2Client ec2) {
        int count = Math.min(max_number_of_workers - number_of_workers, worker_per_message);
        while (count > 0) {
            create_Worker(ec2);
            count--;
        }
        // לזכור שצריך להבחין בין הוורקרים.
    }
    // איך אפשר לדעת האם וורקר נפל?
    // hashmap - key- worker, value- line in the input text
    // 1) איך מתחברים לאותו queue של מנג'ר ולוקל במחלקה מנג'ר
    // 2) איך להוסיף לפרסר את האופציות
    // 3) יש 3 בקטים: מחולקים לפי סוגי הודעות, המפתח זה הid של לוקל אפליקיישן, הערך (נגיד בבאקט של הsummary) יש יותר מערך אחד, כלומר, כל וורקר מכניס חתיכה. מה עושים
    // בתרשים בעבודה- מה המשמעות של queue message?
    // כאשר יצרנו קובץ html, האפליקציה צריכה להפסיק לרוץ?








//    SqsClient sqs = SqsClient.builder().region(Region.US_WEST_2).build();
//    CreateQueueResponse ManagerQueueResFromLocal =
//            sqs.createQueue(CreateQueueRequest.builder().queueName("dsp-local-to-manager-queue").build());
//    String localManagerQueueUrl = localQueueRes.queueUrl(); // a url to a queue from the local app to the manager
//
//    CreateQueueResponse managerQueueRes =
//            sqs.createQueue(CreateQueueRequest.builder().queueName("dsp-manager-to-local-queue").build());
//    String managerLocalQueueUrl = managerQueueRes.queueUrl(); // a url to a queue from the manager to the local app
//    SqsClient sqs = SqsClient.builder().region(Region.US_WEST_2).build();
//    S3Client s3 = S3Client.builder().region(Region.US_WEST_2).build();
//    CreateQueueResponse workerQueueRes =
//            sqs.createQueue(CreateQueueRequest.builder().queueName("dsp-manager-queue").build());
//    CreateQueueResponse managerQueueRes =
//            sqs.createQueue(CreateQueueRequest.builder().queueName("dsp-worker-queue").build());
//    String managerUrl = managerQueueRes.queueUrl();
//    String workerUrl = workerQueueRes.queueUrl();
//
//    InputStream sum = s3.getObject(GetObjectRequest.builder().bucket("summary").key(local_application_id).build());

}
