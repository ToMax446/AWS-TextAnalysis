import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import tools.AWSAbstractions;
import tools.Job;

import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutorService;

import java.util.concurrent.Executors;

import static java.lang.System.exit;

public class Manager {
    static int number_of_workers = 0;
    static int worker_per_message = 0;
    static int max_number_of_workers = 18;
    // NEED TO ADD SYNC
    static int num_of_threads = 10;

    private void bootstraps_Nodes(Ec2Client ec2) {
        int count = Math.min(max_number_of_workers - number_of_workers, worker_per_message);
        while (count > 0) {
            create_Worker(ec2);
            count--;
        }
        // לזכור שצריך להבחין בין הוורקרים.
    }

    private static void terminate() {
    }

    public static void main(String args[]) {
        Ec2Client ec2 = Ec2Client.create(); // connect to EC2 service
        S3Client s3 = S3Client.create(); // connect to S3 service
        SqsClient sqs = SqsClient.create(); // connect to SQS service
        String local_to_manager_url = AWSAbstractions.queue_Setup(sqs, "dsp-local-to-manager-queue");
        String manager_to_local_url = AWSAbstractions.queue_Setup(sqs, "dsp-manager-to-local-queue");
        String manager_to_workers_url = AWSAbstractions.queue_Setup(sqs, "dsp-manager-to-workers-queue");
        String workers_to_manager_url = AWSAbstractions.queue_Setup(sqs, "dsp-workers-to-manager-queue");
        int num_of_jobs = 0;
        ExecutorService executor = Executors.newFixedThreadPool(num_of_threads);
        boolean terminate = false;
        int num_of_local_applications = 0;
        while (!terminate){
//            Job new_job = getJobFromLocal(local_to_manager_url);
            ReceiveMessageRequest request = ReceiveMessageRequest.builder().queueUrl(local_to_manager_url)
                    .maxNumberOfMessages(1).build();    // a request to receive only one message

            List<Message> msg;

            do{
                msg = sqs.receiveMessage(request).messages();
                String[] message = msg.get(0).body().split("\t"); // there is max only one message so get it
                // the statement int localapplication:
                //  sqs.sendMessage(SendMessageRequest.builder().queueUrl(localManagerQueueUrl).messageBody(worker_per_message+"\t"+local_application_id+"\t"+ input_file).build()); //CHECK

                if (n.equals("terminate")) // if the first arg in the message is "terminate" it's a terminate
                    terminate = true;
                // else it's a new job
                String local_application_id = message[1], input_file = message[2];

            } while (msg.isEmpty());
        }


                num_of_local_applications++;
                num_of_jobs++;
                Runnable job = new ManagerTask(new Job(local_application_id, n), ec2, sqs, s3, manager_to_workers_url, workers_to_manager_url);
                executor.execute(job);
                AwaitForAnswer() - non blocking

        }
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


    private static Job getJobFromLocal(String queueUrl, SqsClient sqs) {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder().queueUrl(queueUrl)
                .maxNumberOfMessages(1).build();    // a request to receive only one message

        List<Message> msg;
        do {
            msg = sqs.receiveMessage(request).messages();
            String[] message = msg.get(0).body().split("\t"); // there is max only one message so get it
            // the statement int localapplication:
            //  sqs.sendMessage(SendMessageRequest.builder().queueUrl(localManagerQueueUrl).messageBody(worker_per_message+"\t"+local_application_id+"\t"+ input_file).build()); //CHECK
            String n = message[0];
            if (n.equals("terminate")) // if the first arg in the message is "terminate" it's a terminate
                return null;
            // else it's a new job
            String local_application_id = message[1], input_file = message[2];
            return new Job(local_application_id, Integer.parseInt(n));

        } while (msg.isEmpty());
    }
}
