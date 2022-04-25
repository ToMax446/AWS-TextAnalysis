import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import tools.AWSAbstractions;
import tools.Job;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ManagerTask implements Runnable {
    Ec2Client ec2;
    SqsClient sqs;
    S3Client s3;
    Job job;
    String manager_to_workers_url;
    String workers_to_manager_url;

    public ManagerTask(Job job, Ec2Client ec2, SqsClient sqs, S3Client s3, String manager_to_workers_url, String workers_to_manager_url) {
        this.job = job;
        this.ec2 = ec2;
        this.sqs = sqs;
        this.s3 = s3;
        this.manager_to_workers_url = manager_to_workers_url;
        this.workers_to_manager_url = workers_to_manager_url;

    }
    public void run() {
        int num_of_tasks = 0;
        InputStream input_file = s3.getObject(GetObjectRequest.builder().bucket("input").key(job.get_Local_Application_Id()).build());
        BufferedReader reader = new BufferedReader(new InputStreamReader(input_file));
        while (true) {
            try {
                if (reader.ready()) {
                    int n = job.get_N();
                    int num_of_workers = num_of_tasks / n;
                    if (num_of_tasks % n != 0)
                        num_of_workers++;

                    for (int i = 0; i < num_of_workers; i++) {
                        String user_data =  "#! /bin/bash\n"+"wget https://"+/*PLACEHOLDER*/+"/Worker.jar\n"+"java -jar Worker.jar\n";
                        String workerInstanceId = AWSAbstractions.CreateEC2Instance(ec2, "worker", user_data);
                        AWSAbstractions.addWorker(workerInstanceId);
                    }
                    num_of_tasks++;
                    String line = reader.readLine();
                    String[] message = line.split("\t");
                    String type = message[0];
                    String url = message[1];
                    sqs.sendMessage(SendMessageRequest.builder().queueUrl(manager_to_workers_url)
                            .messageBody(job.get_Local_Application_Id() + "\t" + type + "\t" + url).build());
                }


                String result = AWSAbstractions.wait_For_Answer(sqs, workers_to_manager_url, "done one task", num_of_tasks);



            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
