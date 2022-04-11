import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import static java.lang.System.exit;

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

    public static void delete_Message(String url, Message msg, SqsClient sqs) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(url)
                .receiptHandle(msg.receiptHandle())
                .build();
        sqs.deleteMessage(deleteMessageRequest);
    }

    public static void main(String[] args) {
        SqsClient sqs = SqsClient.builder().region(Region.US_WEST_2).build();
        S3Client s3 = S3Client.builder().region(Region.US_WEST_2).build();
        CreateQueueResponse workerQueueRes =
                sqs.createQueue(CreateQueueRequest.builder().queueName("dsp-manager-queue").build());
        CreateQueueResponse managerQueueRes =
                sqs.createQueue(CreateQueueRequest.builder().queueName("dsp-worker-queue").build());
        String managerUrl = managerQueueRes.queueUrl();
        String workerUrl = workerQueueRes.queueUrl();
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(workerUrl)
                .maxNumberOfMessages(1)
                .build();
        for (; ; ) {
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).messages();
            if (!messages.isEmpty()) {
                String[] message = messages.get(0).body().split("\t");
                // after the parsing, the result is a text file
                try {
                    PutObjectResponse file = s3.putObject(PutObjectRequest.builder().bucket(uniqueBucket).key("ass1").build(), RequestBody.fromFile(new File(parsingRes)));
                    sqs.sendMessage(SendMessageRequest.builder().queueUrl(managerUrl).messageBody("original URL of the input file: " + message[1] + " S3 URL of the analyzed file: " + file + " type of the performed analysis: " + message[0]).build());
                    delete_Message(workerUrl, messages.get(0), sqs);
                } catch (Exception e) {
                    sqs.sendMessage(SendMessageRequest.builder().queueUrl(managerUrl).messageBody("an exception from kind " + e + "has occurred because of the input file " + message[1]).build());
                    delete_Message(workerUrl, messages.get(0), sqs);
                }
            }
        }
    }
}



                //String run = "java -cp stanford-parser.jar:. -mx200m edu.stanford.nlp.parser.lexparser.LexicalizedParser -retainTMPSubcategories -outputFormat \"wordsAndTags,penn,typedDependencies\" englishPCFG.ser.gz input.txt";