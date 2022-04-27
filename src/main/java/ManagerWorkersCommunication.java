import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import tools.AWSAbstractions;
import java.io.*;
import java.nio.file.Path;
import java.util.List;

public class ManagerWorkersCommunication implements Runnable {

    @Override
    public void run() {
        Ec2Client ec2 = AWSAbstractions.getEC2Client();
        SqsClient sqs = AWSAbstractions.getSQSClient();
        S3Client s3 = AWSAbstractions.getS3Client();
        String workersManagerSQS = AWSAbstractions.queueSetup(sqs, "workers-to-manager-queue");
        while (true) {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(workersManagerSQS)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(10)
                    .build();
            List<Message> messages;
            messages = sqs.receiveMessage(receiveMessageRequest).messages();
            if (!messages.isEmpty()) {
                String[] message = messages.get(0).body().split("\t");
                if (message[0].equals("ack task")) { // worker announces it took a message
                    // worker protocol "ack task" + "\t" + workerID + "\t" + messages.get(0).body();
                    String workerID = message[1];
                    String task = message[2];
                    AWSAbstractions.addTask(workerID, task);

                } else if (message[0].equals("done task")) {   //worker announces it finished a task
                    // worker protocol "done task" + "\t" + localAppId + "\t" + file+ "\t"+ type + "\t" + url + "\t" + workerID
                    String localApplicationID = message[1], requiredFile = message[4], typeOfAnalysis = message[3], workerID = message[5];
                    String task = localApplicationID + "\t" + typeOfAnalysis + "\t" + requiredFile;
                    AWSAbstractions.doneTask(localApplicationID, workerID, task); // lock?
                    if (AWSAbstractions.numOfTasks(localApplicationID) == 0) {
                        AWSAbstractions.removeLocalApp(localApplicationID);
                        try {
                            if (Thread.currentThread().isInterrupted()) {
                                for (String worker : AWSAbstractions.getWorkers()) {
                                    ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(worker).build());
                                    System.out.println("terminating worker: " + worker);
                                }
                                break;
                            }
                            InputStream result = new FileInputStream("result.txt");
                            for (String taskID : AWSAbstractions.getTasks(localApplicationID)) {
                                InputStream file = s3.getObject(GetObjectRequest.builder()
                                        .bucket("summary").key(localApplicationID + "/" + taskID).build());
                                new java.io.SequenceInputStream(result, file);

                            }

                            // upload the summary file to s3 and send a message to the local application
                            s3.putObject(PutObjectRequest.builder().bucket("output").key(localApplicationID).build(), RequestBody.fromFile((Path) result));
                            String managerLocalSQS = AWSAbstractions.queueSetup(sqs, "manager-to-local-queue-" + localApplicationID);
                            sqs.sendMessage((SendMessageRequest.builder().queueUrl(managerLocalSQS).messageBody("done job\t" + localApplicationID).build()));

                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }
}
