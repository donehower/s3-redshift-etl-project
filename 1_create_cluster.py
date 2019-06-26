import configparser
import boto3
from botocore.exceptions import ClientError
from redshift_utils import get_client, get_iam_arn, get_cluster_endpoint, modify_config_file

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

CLUSTER_TYPE = config.get('CLUSTER_SETUP', 'CLUSTER_TYPE')
NUM_NODES = config.get('CLUSTER_SETUP', 'NUM_NODES')
NODE_TYPE = config.get('CLUSTER_SETUP', 'NODE_TYPE')

CLUSTER_IDENTIFIER = config.get('CLUSTER_SETUP', 'CLUSTER_IDENTIFIER')
DB_NAME = config.get('CLUSTER', 'DB_NAME')
DB_USER = config.get('CLUSTER', 'DB_USER')
DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
IAM_ROLE_NAME = config.get('CLUSTER_SETUP', 'IAM_ROLE_NAME')
DWH_PORT = config.get('CLUSTER', 'DB_PORT')


def create_cluster(redshift_client_obj, iam_arn_id):
    """
    Creates a Redshift cluster with environment variables provided.
    :param redshift_client_ojb: Client object to access Redshift.
    :param iam_arn_id: Role to assign to the cluster.
    
    :returns: Nothing.  Prints to console if cluster is created without error.
    """
    try:
        response = redshift_client_obj.create_cluster(
            # hardware
            ClusterType=CLUSTER_TYPE,
            NodeType=NODE_TYPE,
            NumberOfNodes=int(NUM_NODES),

            # identifiers and credentials
            DBName=DB_NAME,
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,

            # parameter for role to access S3
            IamRoles=[iam_arn_id]
        )
        print("Cluster created.")
    except Exception as e:
        print(e)


def open_port(ec2_client_obj, redshift_client_obj):
    """
    Opens port (provided by script's environment variable) to ensure cluster is reachable.
    :param ec2_client_obj: EC2 client used to interact with VPC.
    :param redshift_client_obj: Redshift client used to retrieve the VPC ID.
    
    :returns: Nothing.  Prints to console that port is open.
    """
    try:
        cluster_props = redshift_client_obj.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
        vpc = ec2_client_obj.Vpc(id=cluster_props['VpcId'])
        allSg = list(vpc.security_groups.all())
        redshift_sg = [sg for sg in allSg if sg.group_name == 'redshift_security_group'][0]

        redshift_sg.authorize_ingress(GroupName='default',
                                      CidrIp='0.0.0.0/0',
                                      IpProtocol='TCP',
                                      FromPort=int(DWH_PORT),
                                      ToPort=int(DWH_PORT)
                                      )
        print("Port is open")
    except ClientError as e:
        print("Port 5439 already open.")


def main():
    # get IAM arn and write it to the config file
    iam_client = get_client('client', 'iam', 'us-west-2', KEY, SECRET)
    iam_arn = get_iam_arn(iam_client, IAM_ROLE_NAME)
    modify_config_file(config, 'dwh.cfg', 'IAM_ROLE', 'ARN', iam_arn)
    
    # create the cluster
    redshift_client = get_client('client', 'redshift', 'us-west-2', KEY, SECRET)
    create_cluster(redshift_client, iam_arn)
    
    # get the endpoint and write it to the config file
    ep = get_cluster_endpoint(redshift_client, CLUSTER_IDENTIFIER)
    print(ep)
    modify_config_file(config, 'dwh.cfg', 'CLUSTER', 'HOST', ep)
    
    # make sure port 5439 is open to all traffic
    ec2_client = get_client('resource', 'ec2', 'us-west-2', KEY, SECRET)
    open_port(ec2_client, redshift_client)
    
    print('Cluster created and available.')


if __name__ == "__main__":
    main()