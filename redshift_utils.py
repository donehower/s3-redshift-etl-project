import configparser
import boto3


def get_client(sdk_service, resource, region, key, secret):
    """
    Create a client to access an AWS resource.
    :param sdk_service: Access level to use. Must be either 'client' or 'resource'.
    :param resource: AWS service being accessed.  (i.e. 'redshift')
    :param region: Region where resource resides. (i.e. 'us-west-2')
    :param key: Access key of user
    :param secret: Secret key of user
    
    :returns: The client created.
    """
    if sdk_service == 'client':
        return boto3.client(resource,
                            region_name=region,
                            aws_access_key_id=key,
                            aws_secret_access_key=secret)
    elif sdk_service == 'resource':
        return boto3.resource(resource,
                            region_name=region,
                            aws_access_key_id=key,
                            aws_secret_access_key=secret)
    else:
        print("sdk_service needs to be 'client' or resource")


def get_iam_arn(iam_client, role_name):
    """
    Retrieves the complete Amazon Resource name based on provided paramters.
    :param iam_client: IAM client object.
    :param role_name: Name of the role.
    
    :returns:  The AWS ARN in the format: arn:aws:iam::811937106259:role/<role name here>
    """
    try:
        print("Retrieving IAM role ARN")
        return iam_client.get_role(RoleName=role_name)['Role']['Arn']
    except Exception as e:
        print(e)


def get_cluster_endpoint(client_obj, cluster_id):
    """
    Retrieves the endpoint for an available cluster.
    :param client_obj: Redshift client object
    :param cluster_id: Redshift cluster identifier
    
    :returns: The endpoint of the cluster in the format: <cluster_identifier_here>.czo6h37vucef.us-west-2.redshift.amazonaws.com
    """
    waiter = client_obj.get_waiter('cluster_available')
    waiter.wait(
        ClusterIdentifier=cluster_id,
        MaxRecords=20,
        WaiterConfig={
            'Delay': 60,
            'MaxAttempts': 20
            }
    )
    cluster_props = client_obj.describe_clusters(ClusterIdentifier=cluster_id)['Clusters'][0]
    return cluster_props['Endpoint']['Address']


def modify_config_file(config_obj, config_file_name, section_name, option_name, value):
    """
    Modifies a value in the config file.
    :param config_obj: Config parser object
    :param config_file_name: Config file name
    :param section_name: Name of the section to be modified
    :param option_name: Name of the option to be modified
    :param value: The new value to write to the file
    
    :returns: Nothing. Prints to console the section and option modified.
    """
    config_obj.set(section_name, option_name, value)
    with open(config_file_name, 'w') as configfile:
        config_obj.write(configfile)
    print("Config file modified for {}: {}.".format(section_name, option_name))
    return
