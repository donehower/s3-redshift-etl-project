import configparser
import boto3
from redshift_utils import get_client

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
CLUSTER_IDENTIFIER = config.get('CLUSTER_SETUP', 'CLUSTER_IDENTIFIER')

def confirm_deletion(client_obj):
    """
    Polls the cluster ever 60 seconds until 'cluster_deleted' is returned.
    :param client_obj: Client to connect with cluster.
    
    :returns: Nothing.  Prints to cluster once deletion is complete.
    """
    waiter = client_obj.get_waiter('cluster_deleted')
    waiter.wait(
    ClusterIdentifier=CLUSTER_IDENTIFIER
    )
    print("Cluster deleted.")
    return


def main():
    redshift_client = get_client('client', 'redshift', 'us-west-2', KEY, SECRET)
    try:
        redshift_client.delete_cluster(ClusterIdentifier=CLUSTER_IDENTIFIER,
                            SkipFinalClusterSnapshot=True)
        confirm_deletion(redshift_client)
    except redshift_client.exceptions.InvalidClusterStateFault as e:
        print("Operation running on cluster.  Cannot currently delete.")
    
    
if __name__ == "__main__":
    main()