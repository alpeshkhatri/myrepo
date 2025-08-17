from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
from kafka.errors import KafkaError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaTopicConfigManager:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        """
        Initialize Kafka Admin Client
        
        Args:
            bootstrap_servers (list): List of Kafka bootstrap servers
        """
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='topic-config-manager'
        )
    
    def add_topic_config(self, topic_name, config_dict):
        """
        Add or update configurations for a specific topic
        
        Args:
            topic_name (str): Name of the topic
            config_dict (dict): Dictionary of configuration key-value pairs
        """
        try:
            # Create ConfigResource for the topic
            resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
            
            # Prepare configurations to add/update
            configs = {resource: config_dict}
            
            # Apply the configuration changes
            result = self.admin_client.alter_configs(configs)
            
            # Wait for the operation to complete
            for resource, future in result.items():
                try:
                    future.result()  # This will raise an exception if the operation failed
                    logger.info(f"Successfully added/updated configurations for topic '{topic_name}': {config_dict}")
                except Exception as e:
                    logger.error(f"Failed to add/update configurations for topic '{topic_name}': {e}")
                    raise
                    
        except KafkaError as e:
            logger.error(f"Kafka error while adding config to topic '{topic_name}': {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while adding config to topic '{topic_name}': {e}")
            raise
    
    def delete_topic_config(self, topic_name, config_keys):
        """
        Delete specific configurations from a topic (reset to default)
        
        Args:
            topic_name (str): Name of the topic
            config_keys (list): List of configuration keys to delete
        """
        try:
            # Create ConfigResource for the topic
            resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
            
            # Prepare configurations to delete (set to None to delete/reset to default)
            configs_to_delete = {key: None for key in config_keys}
            configs = {resource: configs_to_delete}
            
            # Apply the configuration changes
            result = self.admin_client.alter_configs(configs)
            
            # Wait for the operation to complete
            for resource, future in result.items():
                try:
                    future.result()
                    logger.info(f"Successfully deleted configurations from topic '{topic_name}': {config_keys}")
                except Exception as e:
                    logger.error(f"Failed to delete configurations from topic '{topic_name}': {e}")
                    raise
                    
        except KafkaError as e:
            logger.error(f"Kafka error while deleting config from topic '{topic_name}': {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while deleting config from topic '{topic_name}': {e}")
            raise
    
    def get_topic_config(self, topic_name):
        """
        Retrieve current configurations for a topic
        
        Args:
            topic_name (str): Name of the topic
            
        Returns:
            dict: Dictionary of current topic configurations
        """
        try:
            # Create ConfigResource for the topic
            resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
            
            # Get configurations
            result = self.admin_client.describe_configs([resource])
            
            configs = {}
            for resource, future in result.items():
                try:
                    config_response = future.result()
                    configs = {config.name: config.value for config in config_response.configs}
                    logger.info(f"Retrieved configurations for topic '{topic_name}'")
                except Exception as e:
                    logger.error(f"Failed to retrieve configurations for topic '{topic_name}': {e}")
                    raise
            
            return configs
            
        except KafkaError as e:
            logger.error(f"Kafka error while retrieving config for topic '{topic_name}': {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while retrieving config for topic '{topic_name}': {e}")
            raise
    
    def close(self):
        """Close the admin client connection"""
        if hasattr(self, 'admin_client'):
            self.admin_client.close()


# Example usage
if __name__ == "__main__":
    # Initialize the config manager
    config_manager = KafkaTopicConfigManager(['localhost:9092'])
    
    topic_name = "my-test-topic"
    
    try:
        # Example 1: Add/Update topic configurations
        print("Adding topic configurations...")
        new_configs = {
            'retention.ms': '86400000',  # 1 day retention
            'cleanup.policy': 'delete',
            'max.message.bytes': '1048576',  # 1MB max message size
            'min.insync.replicas': '2'
        }
        config_manager.add_topic_config(topic_name, new_configs)
        
        # Example 2: Get current topic configurations
        print("\nRetrieving current topic configurations...")
        current_configs = config_manager.get_topic_config(topic_name)
        print(f"Current configs for {topic_name}:")
        for key, value in current_configs.items():
            print(f"  {key}: {value}")
        
        # Example 3: Delete specific configurations (reset to default)
        print(f"\nDeleting specific configurations from topic...")
        configs_to_delete = ['retention.ms', 'max.message.bytes']
        config_manager.delete_topic_config(topic_name, configs_to_delete)
        
        # Example 4: Verify configurations after deletion
        print("\nVerifying configurations after deletion...")
        updated_configs = config_manager.get_topic_config(topic_name)
        print(f"Updated configs for {topic_name}:")
        for key, value in updated_configs.items():
            print(f"  {key}: {value}")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the connection
        config_manager.close()


# Additional utility functions for common configurations
def set_topic_retention(topic_name, retention_ms, bootstrap_servers=['localhost:9092']):
    """
    Utility function to set topic retention policy
    
    Args:
        topic_name (str): Name of the topic
        retention_ms (int): Retention time in milliseconds
        bootstrap_servers (list): Kafka bootstrap servers
    """
    config_manager = KafkaTopicConfigManager(bootstrap_servers)
    try:
        config_manager.add_topic_config(topic_name, {'retention.ms': str(retention_ms)})
        print(f"Set retention for topic '{topic_name}' to {retention_ms}ms")
    finally:
        config_manager.close()


def set_topic_cleanup_policy(topic_name, policy='delete', bootstrap_servers=['localhost:9092']):
    """
    Utility function to set topic cleanup policy
    
    Args:
        topic_name (str): Name of the topic
        policy (str): 'delete' or 'compact' or 'delete,compact'
        bootstrap_servers (list): Kafka bootstrap servers
    """
    config_manager = KafkaTopicConfigManager(bootstrap_servers)
    try:
        config_manager.add_topic_config(topic_name, {'cleanup.policy': policy})
        print(f"Set cleanup policy for topic '{topic_name}' to '{policy}'")
    finally:
        config_manager.close()


def reset_topic_config(topic_name, config_keys, bootstrap_servers=['localhost:9092']):
    """
    Utility function to reset topic configurations to default
    
    Args:
        topic_name (str): Name of the topic
        config_keys (list): List of configuration keys to reset
        bootstrap_servers (list): Kafka bootstrap servers
    """
    config_manager = KafkaTopicConfigManager(bootstrap_servers)
    try:
        config_manager.delete_topic_config(topic_name, config_keys)
        print(f"Reset configurations for topic '{topic_name}': {config_keys}")
    finally:
        config_manager.close()
