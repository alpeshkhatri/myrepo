#!/bin/bash

# Kafka Partition Replication Order Change Script
# This script helps change the replication order of Kafka topic partitions

# Configuration
KAFKA_HOME="/opt/kafka"  # Adjust to your Kafka installation path
BOOTSTRAP_SERVERS="localhost:9092"  # Adjust to your Kafka brokers
TOPIC_NAME="your-topic-name"  # Change to your topic name

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Kafka Partition Replication Order Change Script${NC}"
echo "=============================================="

# Function to check if Kafka tools are available
check_kafka_tools() {
    if [ ! -f "$KAFKA_HOME/bin/kafka-reassign-partitions.sh" ]; then
        echo -e "${RED}Error: Kafka tools not found at $KAFKA_HOME${NC}"
        echo "Please update KAFKA_HOME variable or ensure Kafka is installed"
        exit 1
    fi
}

# Function to get current topic description
get_current_state() {
    echo -e "${YELLOW}Current partition assignment for topic: $TOPIC_NAME${NC}"
    $KAFKA_HOME/bin/kafka-topics.sh \
        --bootstrap-server $BOOTSTRAP_SERVERS \
        --describe \
        --topic $TOPIC_NAME
    echo ""
}

# Function to generate reassignment JSON
generate_reassignment_json() {
    echo -e "${YELLOW}Generating current partition assignment JSON...${NC}"
    
    # Create topics-to-move.json
    cat > topics-to-move.json << EOF
{
    "topics": [
        {"topic": "$TOPIC_NAME"}
    ],
    "version": 1
}
EOF

    # Generate current assignment
    $KAFKA_HOME/bin/kafka-reassign-partitions.sh \
        --bootstrap-server $BOOTSTRAP_SERVERS \
        --topics-to-move-json-file topics-to-move.json \
        --broker-list "0,1,2" \
        --generate > reassignment-output.txt

    # Extract the current assignment
    grep -A 1000 "Current partition replica assignment" reassignment-output.txt | \
    grep -B 1000 "Proposed partition reassignment configuration" | \
    head -n -2 | tail -n +2 > current-assignment.json

    echo -e "${GREEN}Current assignment saved to current-assignment.json${NC}"
}

# Function to create custom reassignment
create_custom_reassignment() {
    echo -e "${YELLOW}Creating custom reassignment configuration...${NC}"
    
    # Example reassignment - modify this based on your needs
    # This example changes the replica order for a 3-partition topic with 3 replicas each
    cat > custom-reassignment.json << 'EOF'
{
    "version": 1,
    "partitions": [
        {
            "topic": "your-topic-name",
            "partition": 0,
            "replicas": [1, 2, 0],
            "log_dirs": ["any", "any", "any"]
        },
        {
            "topic": "your-topic-name",
            "partition": 1,
            "replicas": [2, 0, 1],
            "log_dirs": ["any", "any", "any"]
        },
        {
            "topic": "your-topic-name",
            "partition": 2,
            "replicas": [0, 1, 2],
            "log_dirs": ["any", "any", "any"]
        }
    ]
}
EOF

    # Replace placeholder topic name with actual topic name
    sed -i "s/your-topic-name/$TOPIC_NAME/g" custom-reassignment.json
    
    echo -e "${GREEN}Custom reassignment configuration created: custom-reassignment.json${NC}"
    echo -e "${YELLOW}Please review and modify the replica assignments as needed.${NC}"
}

# Function to execute reassignment
execute_reassignment() {
    echo -e "${YELLOW}Executing partition reassignment...${NC}"
    
    $KAFKA_HOME/bin/kafka-reassign-partitions.sh \
        --bootstrap-server $BOOTSTRAP_SERVERS \
        --reassignment-json-file custom-reassignment.json \
        --execute
}

# Function to verify reassignment
verify_reassignment() {
    echo -e "${YELLOW}Verifying reassignment progress...${NC}"
    
    $KAFKA_HOME/bin/kafka-reassign-partitions.sh \
        --bootstrap-server $BOOTSTRAP_SERVERS \
        --reassignment-json-file custom-reassignment.json \
        --verify
}

# Function to monitor reassignment progress
monitor_progress() {
    echo -e "${YELLOW}Monitoring reassignment progress...${NC}"
    echo "Press Ctrl+C to stop monitoring"
    
    while true; do
        clear
        echo -e "${GREEN}Reassignment Status Check - $(date)${NC}"
        echo "========================================="
        
        verify_reassignment
        
        echo ""
        echo "Checking again in 10 seconds..."
        sleep 10
    done
}

# Main menu
show_menu() {
    echo ""
    echo "Choose an option:"
    echo "1) Show current partition assignment"
    echo "2) Generate current assignment JSON"
    echo "3) Create custom reassignment configuration" 
    echo "4) Execute reassignment"
    echo "5) Verify reassignment"
    echo "6) Monitor reassignment progress"
    echo "7) Exit"
    echo ""
}

# Main script execution
main() {
    check_kafka_tools
    
    while true; do
        show_menu
        read -p "Enter your choice (1-7): " choice
        
        case $choice in
            1)
                get_current_state
                ;;
            2)
                generate_reassignment_json
                ;;
            3)
                create_custom_reassignment
                echo -e "${YELLOW}Note: Edit custom-reassignment.json to specify your desired replica order${NC}"
                ;;
            4)
                if [ -f "custom-reassignment.json" ]; then
                    read -p "Are you sure you want to execute the reassignment? (y/N): " confirm
                    if [[ $confirm =~ ^[Yy]$ ]]; then
                        execute_reassignment
                    else
                        echo "Reassignment cancelled."
                    fi
                else
                    echo -e "${RED}Error: custom-reassignment.json not found. Create it first (option 3).${NC}"
                fi
                ;;
            5)
                if [ -f "custom-reassignment.json" ]; then
                    verify_reassignment
                else
                    echo -e "${RED}Error: custom-reassignment.json not found.${NC}"
                fi
                ;;
            6)
                if [ -f "custom-reassignment.json" ]; then
                    monitor_progress
                else
                    echo -e "${RED}Error: custom-reassignment.json not found.${NC}"
                fi
                ;;
            7)
                echo -e "${GREEN}Goodbye!${NC}"
                exit 0
                ;;
            *)
                echo -e "${RED}Invalid option. Please choose 1-7.${NC}"
                ;;
        esac
        
        read -p "Press Enter to continue..."
    done
}

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up temporary files...${NC}"
    rm -f topics-to-move.json reassignment-output.txt
}

# Set trap for cleanup on script exit
trap cleanup EXIT

# Run main function
main
