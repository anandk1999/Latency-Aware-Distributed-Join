# #!/bin/bash

# # Variables
# DRIVER_IP="192.168.10.2" # Replace with the actual driver IP
# WORKER_IPS=("192.168.10.3" "192.168.10.4") # Replace with the actual worker IPs
# DELAY="100ms" # Induced latency (e.g., 100ms)
# INTERFACE="eth0" # Network interface to apply the latency (e.g., eth0)

# # Function to add latency
# add_latency() {
#     local target_ip=$1
#     echo "Inducing latency of $DELAY to $target_ip on interface $INTERFACE"
#     sudo tc qdisc add dev $INTERFACE root handle 1: prio
#     sudo tc filter add dev $INTERFACE protocol ip parent 1:0 prio 1 u32 match ip dst $target_ip flowid 1:1
#     sudo tc qdisc add dev $INTERFACE parent 1:1 handle 10: netem delay $DELAY
# }

# # Function to remove latency
# remove_latency() {
#     echo "Removing latency from interface $INTERFACE"
#     sudo tc qdisc del dev $INTERFACE root
# }

# # Check arguments
# if [[ $1 == "add" ]]; then
#     echo "Adding latency between Driver and Workers..."
#     for WORKER_IP in "${WORKER_IPS[@]}"; do
#         add_latency "$WORKER_IP"
#     done
#     echo "Latency added successfully."
# elif [[ $1 == "remove" ]]; then
#     echo "Removing latency between Driver and Workers..."
#     remove_latency
#     echo "Latency removed successfully."
# else
#     echo "Usage: $0 {add|remove}"
#     exit 1
# fi


###########################


#!/bin/bash

# Define IPs for the master and workers
MASTER_IP="192.168.10.2"
WORKER1_IP="192.168.10.3"
WORKER2_IP="192.168.10.4"

# Define latency (in milliseconds) for each pair
LATENCY_MASTER_WORKER1="1000ms"
LATENCY_MASTER_WORKER2="1500ms"

# Check if existing latency is already applied and remove it
EXISTING_QDISC=$(tc qdisc show dev eth0 | grep -o 'netem')

# If traffic control is already applied, delete it
if [ -n "$EXISTING_QDISC" ]; then
    echo "Existing traffic control configuration found, removing it..."
    sudo tc qdisc del dev eth0 root
fi

# Apply latency between Master and Worker 1 (Master → Worker 1)
echo "Inducing latency of $LATENCY_MASTER_WORKER1 in the network"
sudo tc qdisc add dev eth0 root netem delay $LATENCY_MASTER_WORKER1



# Optional: Apply IP-specific filtering if you want to limit latency to certain traffic
# Apply latency specifically for traffic between Master and Worker 1 (based on destination IP)
# sudo tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst $WORKER1_IP flowid 1:1

# Apply latency specifically for traffic between Master and Worker 2 (based on destination IP)
# sudo tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst $WORKER2_IP flowid 1:1 

echo "Latency successfully applied between Master and Workers."
