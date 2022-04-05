import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.System.exit;

public class Worker {

    public static void main(String[] args) {
        SqsClient sqs = SqsClient.builder().region(Region.US_WEST_2).build();

        for (;;)    // loop until we get a message
        {
            List<Message> messages =
                    sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(managerLocalQueueUrl).build()).messages();
            if (!messages.isEmpty()) {
                String[] message = messages.get(0).body().split("__");
                if (message[0].equals("analysis task")) {
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
