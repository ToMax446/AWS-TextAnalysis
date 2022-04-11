import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.System.exit;

public class Worker {

    public static void main(String[] args) {
        SqsClient sqs = SqsClient.builder().region(Region.US_WEST_2).build();
        S3Client s3 = S3Client.builder().region(Region.US_WEST_2).build();
        for (; ; )    // loop until we get a message
        {
            CreateQueueResponse workerQueueRes =
                    sqs.createQueue(CreateQueueRequest.builder().queueName("dsp-from-worker-to-manager-queue").build());
            CreateQueueResponse managerQueueRes =
                    sqs.createQueue(CreateQueueRequest.builder().queueName("dsp-from-manager-to-worker-queue").build());
            String fromManagerToWorkerQueueUrl = managerQueueRes.queueUrl();
            String fromWorkerToManagerQueueUrl = workerQueueRes.queueUrl();
            List<Message> messages =
                    sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(fromManagerToWorkerQueueUrl).build()).messages();
            if (!messages.isEmpty()) {
                String[] message = messages.get(0).body().split("__");
//                message: "analysis task__bucket__key"
                if (message[0].equals("analysis task")) {
                    InputStream sum = s3.getObject(GetObjectRequest.builder().bucket(message[1]).key(message[2]).build());
                }
            }
            // send a message from the worker to the manager.
            sqs.sendMessage(SendMessageRequest.builder().queueUrl(fromWorkerToManagerQueueUrl).messageBody("done OCR task" + Delimiter + parts1[0] + Delimiter + text + Delimiter + parts1[1]).build());
            // delete the message f
