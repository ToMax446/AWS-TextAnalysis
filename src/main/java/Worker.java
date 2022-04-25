import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import tools.AWSAbstractions;
import tools.Parser;

import java.io.*;
import java.util.List;

//        ▪ Gets a message from an SQS queue. V
//        ▪ Downloads the text file indicated in the message. V
//        ▪ Performs the requested analysis on the file.
//        ▪ Uploads the resulting analysis file to S3.
//        ▪ Puts a message in an SQS queue indicating the original URL of the input file, the S3 url of the
//          analyzed file, and the type of the performed analysis.
//        ▪ Removes the processed message from the SQS queue.
//        ▪ If an exception occurs, the worker should recover from it, send a message to the manager of
//        the input message that caused the exception together with a short description of the
//        exception, and continue working on the next message.
//        ▪ If a worker stops working unexpectedly before finishing its work on a message, some other
//        worker should be able to handle that message.

public class Worker {
    private static String ManagerWorkersSQS;
    private static String WorkersManagerSQS;
    private static SqsClient sqs;
    private static S3Client s3;
    private static List<Message> MessageQueue;

    private static String parserLocation = "";


    public static void DeleteMessage(String url, Message msg, SqsClient sqs) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(url)
                .receiptHandle(msg.receiptHandle())
                .build();
        sqs.deleteMessage(deleteMessageRequest);
    }

    public static void main(String[] args) {
        sqs = AWSAbstractions.getSQSClient();
        s3 = AWSAbstractions.getS3Client();
        ManagerWorkersSQS = AWSAbstractions.QueueSetup(sqs, "manager-to-workers-queue");
        WorkersManagerSQS = AWSAbstractions.QueueSetup(sqs, "workers-to-manager-queue");
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(ManagerWorkersSQS)
                .maxNumberOfMessages(1)
                .build();
        for (; ; ) {
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
            if (!messages.isEmpty()) {
                String[] message = messages.get(0).body().split("\t");  // take a message (task) from the queue
                String workerId = EC2MetadataUtils.getInstanceId(); // should give worker id
                String ackMsg = "ack task\t"+workerId+"\t"+ messages.get(0).body();  // acknowledge message for manager
                sqs.sendMessage(SendMessageRequest.builder().queueUrl(ManagerWorkersSQS).messageBody(ackMsg).build());  // tell manager what task we took
                // message[0] = local application ID, message[1] = type of analysis type, message[2] = url of input file
                try {
                    PutObjectResponse file = s3.putObject(PutObjectRequest.builder().bucket("summary").key(message[0]).build(), RequestBody.fromFile(new File(parsing_result)));

                    // send message to the manager
//                    AWSAbstractions.addTask();
//                    Parser.parseFile();*/
                    sqs.sendMessage(SendMessageRequest.builder().queueUrl(WorkersManagerSQS).messageBody("done task" + "\t" + message[0] + "\t" + message[2]+ "\t"+ file + "\t" + message[1]).build());

                    // delete the message from the queue
                    DeleteMessage(ManagerWorkersSQS, messages.get(0), sqs);
                } catch (Exception e) {
                    sqs.sendMessage(SendMessageRequest.builder().queueUrl(WorkersManagerSQS).messageBody(e + message[2]).build());

                    // delete the message from the queue
                    DeleteMessage(ManagerWorkersSQS, messages.get(0), sqs);
                }
            }
        }
    }
}