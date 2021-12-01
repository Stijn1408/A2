from kafka.admin import KafkaAdminClient, NewTopic

def create_topics(admin, topic_list):
    admin.create_topics(new_topics=topic_list, validate_only=False)

# Create topic price and connect to virtual machine
if __name__ == '__main__':
    admin_client = KafkaAdminClient(bootstrap_servers="34.70.54.211",
                                    client_id='Assignment2')  # Use your VM's external IP address here
    topic_list = [NewTopic(name="price", num_partitions=1, replication_factor=1)]

    create_topics(admin_client, topic_list)
