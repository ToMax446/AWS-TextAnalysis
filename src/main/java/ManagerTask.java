import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import tools.AWSAbstractions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ManagerTask implements Runnable {
    Ec2Client ec2;
    SqsClient sqs;
    S3Client s3;
    String managerWorkersSQS;
    String workersManagerSQS;
    String localApplicationID;
    int n;

    public ManagerTask(Ec2Client ec2, SqsClient sqs, S3Client s3, String managerWorkersSQS, String workersManagerSQS, String localApplicationID, int n) {
        this.ec2 = ec2;
        this.sqs = sqs;
        this.s3 = s3;
        this.managerWorkersSQS = managerWorkersSQS;
        this.workersManagerSQS = workersManagerSQS;
        this.localApplicationID = localApplicationID;
        this.n = n;

    }

    public void run() {
        InputStream inputFile = s3.getObject(GetObjectRequest.builder()
                .bucket("input").key(localApplicationID).build());

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputFile));
        AWSAbstractions.addLocalApp(localApplicationID);
            try {
                while (reader.ready()) {
                    String line = reader.readLine();
                    String[] message = line.split("\t");
                    String typeOfAnalysis = message[0];
                    String requiredFile = message[1];
                    AWSAbstractions.incNumOfTasks(localApplicationID);
                    sqs.sendMessage(SendMessageRequest.builder().queueUrl(managerWorkersSQS)
                            .messageBody(localApplicationID + "\t" + typeOfAnalysis + "\t" + requiredFile).build());
                }

                int activeWorkers = AWSAbstractions.getWorkersSize();
                int numOfTasks = AWSAbstractions.numOfTasks(localApplicationID);
                int workersForJob = numOfTasks / n;
                int maxSizeOfWorkers = 18;
                int numOfWorkers = Math.min((workersForJob - activeWorkers), (maxSizeOfWorkers - activeWorkers));

                for (int i = 0; i < numOfWorkers; i++) {
                    String user_data = "#! /bin/bash\n" + "wget https://" +/*PLACEHOLDER*/+"/Worker.jar\n" + "java -jar Worker.jar\n";
                    String workerInstanceId = AWSAbstractions
                            .createEC2Instance(ec2, "worker", user_data);
                    AWSAbstractions.addWorker(workerInstanceId);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
