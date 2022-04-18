package tools;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;

public class Job {
    private String local_application_id;
    private int n;

    public Job(String local_application_id, int n) {
        this.local_application_id = local_application_id;
        this.n = n;
    }

    public String get_Local_Application_Id() {
        return local_application_id;
    }

    public int get_N() {
        return n;
    }
}
