package tools;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

public class AWSAbstractions {

    public static String wait_For_Answer(SqsClient sqs,String queueURL,String waitFor,int numofmessages) {
        String result = "";
        int counter = 0;
        while (counter < numofmessages) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueURL)
                    .maxNumberOfMessages(1)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            for (Message message : messages) {
                String done = message.body();
                if (done != null) {
                    String[] msg = done.split("\t");
                    if (msg[0].equals(waitFor)) {
                        counter++;
                        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                                .queueUrl(queueURL)
                                .receiptHandle(message.receiptHandle())
                                .build();
                        sqs.deleteMessage(deleteMessageRequest);
                    }
                } else {
                    System.out.println("class Msg fail");
                }
            }
        }
        return result;
    }

    public static String create_EC2_Instance(Ec2Client ec2, String name, String amiId, String user_data) {
        IamInstanceProfileSpecification role = IamInstanceProfileSpecification.builder()
                .arn("").build();

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(1)
                .minCount(1)
                .iamInstanceProfile(role)
                .userData(user_data)
                .keyName("ec2-ass1")
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("Name")
                .value(name)
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
            System.exit(1);
        }
        return "";
    }

    public static String queue_Setup(SqsClient sqs, String queueName) {
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
            throw e;

        }

        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        System.out.println("Queue url initiallized");

        return queueUrl;
    }
}
