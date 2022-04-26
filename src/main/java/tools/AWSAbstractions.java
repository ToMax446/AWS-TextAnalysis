package tools;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class AWSAbstractions {
    private static ConcurrentHashMap<String, List<String>> workers;
    private static ConcurrentHashMap<String, Integer> localAppTasks; // localAppId to num of tasks
    private static ConcurrentHashMap<String, List<String>> tasksID;
    private static String amiId = "ami-076515f20540e6e0b";

    public static void reassignTasks(String id) {
        SqsClient sqs = getSQSClient();
        String ManagerWorkersSQS = AWSAbstractions.queueSetup(sqs, "manager-to-workers-queue");
        List<String> tasks = workers.get(id);
        for (String task: tasks){
            sqs.sendMessage(SendMessageRequest.builder().queueUrl(ManagerWorkersSQS).messageBody(task).build());
        }
        workers.remove(id);
    }

    public static void addTaskID(String localApplicationID, String taskID) {
        tasksID.get(localApplicationID).add(taskID);
    }

    public static List<String> getTasks(String id) {
        return tasksID.get(id);
    }
    
    public static void addWorker(String id){
        workers.put(id, new ArrayList());
    }

    public static void addTask(String id, String task){
        workers.get(id).add(task);
    }

    public static List<String> getWorkers(){
        return new ArrayList<>(workers.keySet());
    }

    public static int getWorkersSize(){
        return workers.size();
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
