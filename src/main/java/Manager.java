import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import tools.AWSAbstractions;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Manager {

    private static String localManagerSQS;
    private static String managerWorkersSQS;
    private static String workersManagerSQS;
    // NEED TO ADD SYNC
    private static int numOfThreads = 10;

    public static void main(String args[]) {
        Ec2Client ec2 = Ec2Client.create(); // connect to EC2 service
        S3Client s3 = S3Client.create(); // connect to S3 service
        SqsClient sqs = SqsClient.create(); // connect to SQS service
        localManagerSQS = AWSAbstractions.queueSetup(sqs, "local-to-manager-queue");
        managerWorkersSQS = AWSAbstractions.queueSetup(sqs, "manager-to-workers-queue");
        workersManagerSQS = AWSAbstractions.queueSetup(sqs, "workers-to-manager-queue");
        ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);
        boolean terminate = false;

        Runnable workerConnection = new WorkerConnection();
        Runnable managerWorkerCommunication = new ManagerWorkersCommunication();
        executor.execute(workerConnection);
        executor.execute(managerWorkerCommunication);
        while (!terminate) {

            // a request to receive only one message
            ReceiveMessageRequest request = ReceiveMessageRequest.builder().queueUrl(localManagerSQS)
                    .maxNumberOfMessages(1).build();
            List<Message> messages;
            messages = sqs.receiveMessage(request).messages();

            String[] message = messages.get(0).body().split("\t");
            // the statement int localapplication:

//            sqs.sendMessage(SendMessageRequest.builder().queueUrl(localManagerSQS).messageBody("new task" + "\t" + n + "\t" + localApplocationId + "\t" + fileLocation).build());
            if (message[0].equals("terminate")) {
                terminate = true;
                executor.shutdown();
                // wait for shutdown to finish
//                Terminate managerID
            }

            else if (message[1].equals("new job")) {
                int n = Integer.parseInt(message[0]);
                String localApplicationID = message[1];
                String inputFile = message[2];
                Runnable job = new ManagerTask(ec2, sqs, s3, workersManagerSQS, managerWorkersSQS, localApplicationID, n);
                executor.execute(job);
            }
        }
    }
}
