# Use the official ROS 2 Humble base image
FROM ros:humble-ros-base

# Install Python 3, pip, and the tf_transformations ROS package
RUN apt-get update && apt-get install -y \
    python3-pip \
    ros-humble-tf-transformations \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /root/bintobag/build

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy project files into the container
COPY bin_to_ros2bag.py .
COPY common/livox_pb2.py .
COPY common/orientation_pb2.py .

# Default command
CMD ["python3", "bin_to_ros2bag.py", "/root/bin_logs", "/root/bintobag/build/ros2_bags_output"]