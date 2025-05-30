#!/usr/bin/env python3
import rclpy
from rclpy.serialization import serialize_message
from sensor_msgs.msg import PointCloud2, PointField, Imu
from std_msgs.msg import Header
from geometry_msgs.msg import Quaternion
import tf_transformations 

import os
import sys
import glob
import struct
import math
import time
from collections import deque, Counter
import shutil

try:
    import livox_pb2
    import orientation_pb2
except ImportError as e:
    print(f"Error: Could not import protobuf modules. Make sure livox_pb2.py and orientation_pb2.py are accessible.")
    print(f"Details: {e}")
    sys.exit(1)

import rosbag2_py
import numpy as np

# --- Constants ---
GRAVITY_ACCEL = 9.80665  # m/s^2
MAX_ORIENTATION_AGE_MS = 200 # Max age of orientation data to use (200ms, if Duro is ~10Hz, this allows missing one or two)
IMU_BUFFER_MAX_LEN = 100 

LIDAR_FRAME_ID = "lidar"
IMU_FRAME_ID = "imu"

LIDAR_TOPIC = "/os_cloud_node/points"
IMU_TOPIC = "/os_cloud_node/imu"

# Buffers for IMU data components
duro_orientation_buffer = deque(maxlen=IMU_BUFFER_MAX_LEN) # Store most recent orientations


def parse_livox_point_packet(raw_bytes, bin_envelope_timestamp_ms):
    """
    Parses Livox Point_Packet protobuf.
    The PointCloud2.header.stamp will be bin_envelope_timestamp_ms.
    The per-point 'time' field will be packet.timestamp_offset_us[i] (converted to seconds),
    which is relative to the start of the sensor data capture for that frame.
    """
    try:
        packet = livox_pb2.Point_Packet()
        packet.ParseFromString(raw_bytes)

        num_points = packet.num_points
        if not (len(packet.x_coords) == num_points and \
                len(packet.y_coords) == num_points and \
                len(packet.z_coords) == num_points and \
                len(packet.reflectivity) == num_points and \
                len(packet.timestamp_offset_us) == num_points):
            print(f"DEBUG Points: Point_Packet field length mismatch for proto_ts {packet.system_timestamp_us}. Num_points: {num_points}, "
                  f"x: {len(packet.x_coords)}, y: {len(packet.y_coords)}, z: {len(packet.z_coords)}, "
                  f"refl: {len(packet.reflectivity)}, ts_offset: {len(packet.timestamp_offset_us)}. Skipping packet.")
            return None

        points_data_for_ros = [] 
        for i in range(num_points):
            x = packet.x_coords[i] / 1000.0
            y = packet.y_coords[i] / 1000.0
            z = packet.z_coords[i] / 1000.0
            intensity = float(packet.reflectivity[i])
            # Per-point time is the offset from the start of this frame's sensor data capture, in seconds.
            point_time_offset_sec = packet.timestamp_offset_us[i] / 1_000_000.0
            points_data_for_ros.append([x, y, z, intensity, point_time_offset_sec])
        
        ros_header_time_sec = bin_envelope_timestamp_ms / 1000.0
        sec_hdr = int(ros_header_time_sec)
        nanosec_hdr = int((ros_header_time_sec - sec_hdr) * 1_000_000_000)
        
        # print(f"Successfully parsed avia_points with {num_points} points, using bin_ts {bin_envelope_timestamp_ms}")
        return {
            'sec': sec_hdr,
            'nanosec': nanosec_hdr,
            'points_data': points_data_for_ros,
            # 'proto_timestamp_us': packet.timestamp_us # Sensor time of frame start, for reference
        }
    except Exception as e:
        print(f"Error parsing Livox Point_Packet: {e}")
        return None

def create_pointcloud2_msg(parsed_data, frame_id):
    if not parsed_data or not parsed_data['points_data']:
        return None

    header = Header()
    header.stamp.sec = parsed_data['sec']
    header.stamp.nanosec = parsed_data['nanosec']
    header.frame_id = frame_id

    points_np = np.array(parsed_data['points_data'], dtype=np.float32) 
    
    msg = PointCloud2()
    msg.header = header
    msg.height = 1
    msg.width = points_np.shape[0] if len(points_np.shape) > 1 else 0
    if msg.width == 0 and len(parsed_data['points_data']) > 0 : # Handle case of single point
         msg.width = 1
         points_np = points_np.reshape(1, -1)


    msg.is_dense = True 
    msg.is_bigendian = False
    
    msg.fields = [
        PointField(name='x', offset=0, datatype=PointField.FLOAT32, count=1),
        PointField(name='y', offset=4, datatype=PointField.FLOAT32, count=1),
        PointField(name='z', offset=8, datatype=PointField.FLOAT32, count=1),
        PointField(name='intensity', offset=12, datatype=PointField.FLOAT32, count=1),
        PointField(name='time', offset=16, datatype=PointField.FLOAT32, count=1) 
    ]
    msg.point_step = 20 
    msg.row_step = msg.point_step * msg.width
    msg.data = points_np.tobytes()
    
    return msg

def parse_livox_imu_data(raw_bytes, bin_envelope_timestamp_ms):
    packet = None
    try:
        packet = livox_pb2.Imu_Data()
        packet.ParseFromString(raw_bytes)
        # print(f"DEBUG LivoxIMU: Parsed avia_imu, sys_ts_us {packet.system_timestamp_us}, bin_ms {bin_envelope_timestamp_ms}")
        return {
            'bin_timestamp_ms': bin_envelope_timestamp_ms, # Unix epoch ms from logger
            'angular_velocity': [packet.gyro_x, packet.gyro_y, packet.gyro_z], 
            'linear_acceleration': [
                packet.acc_x * GRAVITY_ACCEL, 
                packet.acc_y * GRAVITY_ACCEL,
                packet.acc_z * GRAVITY_ACCEL
            ]
        }
    except Exception as e:
        sys_ts_val = packet.system_timestamp_us if packet and hasattr(packet, 'system_timestamp_us') else "N/A"
        print(f"Error parsing Livox Imu_Data. SysTS_us: {sys_ts_val}. BinTS_ms: {bin_envelope_timestamp_ms}. Exception: {e}")
        return None

def parse_duro_orient_euler(raw_bytes, bin_envelope_timestamp_ms):
    packet = None
    try:
        packet = orientation_pb2.MsgOrientEuler()
        packet.ParseFromString(raw_bytes)
        
        roll_rad = math.radians(packet.roll / 1_000_000.0)
        pitch_rad = math.radians(packet.pitch / 1_000_000.0)
        yaw_rad = math.radians(packet.yaw / 1_000_000.0)
        
        q = tf_transformations.quaternion_from_euler(roll_rad, pitch_rad, yaw_rad) 

        # print(f"DEBUG DuroEuler: Parsed duro_gps_orient_eule, proto_tow_ms {packet.tow}, bin_ms {bin_envelope_timestamp_ms}")
        return {
            'bin_timestamp_ms': bin_envelope_timestamp_ms, # Unix epoch ms from logger
            'orientation_q': [q[0], q[1], q[2], q[3]] 
        }
    except Exception as e:
        tow_val = packet.tow if packet and hasattr(packet, 'tow') else 'N/A'
        print(f"ERROR Parsing Duro Orient Euler. ProtoData: tow={tow_val}. BinTS_ms: {bin_envelope_timestamp_ms}. Exception: {e}")
        return None

def try_combine_imu_data_new(current_livox_imu_data, writer):
    """
    Called when a new Livox IMU (gyro/accel) packet is parsed.
    Tries to find the most recent suitable Duro orientation.
    """
    if not duro_orientation_buffer: # No orientation data available yet
        # print(f"DEBUG IMU Combine: No Duro orientation data in buffer to match with Livox IMU at bin_ms {current_livox_imu_data['bin_timestamp_ms']}")
        return 0

    best_duro_data = None
    # Find the most recent Duro orientation that is NOT NEWER than the current Livox IMU packet,
    # and also not too old.
    for d_data in reversed(duro_orientation_buffer): # Iterate from newest to oldest
        if d_data['bin_timestamp_ms'] <= current_livox_imu_data['bin_timestamp_ms']: # Orientation is not in the future
            if (current_livox_imu_data['bin_timestamp_ms'] - d_data['bin_timestamp_ms']) <= MAX_ORIENTATION_AGE_MS:
                best_duro_data = d_data
                break # Found the best one
            else:
                # print(f"DEBUG IMU Combine: Stale Duro data found (age {current_livox_imu_data['bin_timestamp_ms'] - d_data['bin_timestamp_ms']}ms) for Livox IMU at bin_ms {current_livox_imu_data['bin_timestamp_ms']}")
                break # Older ones will be even more stale

    if best_duro_data:
        imu_msg = create_combined_imu_msg(current_livox_imu_data, best_duro_data, IMU_FRAME_ID)
        if imu_msg:
            ros_timestamp_ns = current_livox_imu_data['bin_timestamp_ms'] * 1_000_000
            writer.write(IMU_TOPIC, serialize_message(imu_msg), ros_timestamp_ns)
            # print(f"DEBUG IMU Combine: SUCCESS! Livox_bin_ms={current_livox_imu_data['bin_timestamp_ms']}, Duro_bin_ms={best_duro_data['bin_timestamp_ms']}")
            return 1
    # else:
        # print(f"DEBUG IMU Combine: No suitable Duro orientation found for Livox IMU at bin_ms {current_livox_imu_data['bin_timestamp_ms']}")
    return 0

def create_combined_imu_msg(livox_data, duro_data, frame_id):
    msg = Imu()
    sec = livox_data['bin_timestamp_ms'] // 1000
    nanosec = (livox_data['bin_timestamp_ms'] % 1000) * 1_000_000
    msg.header.stamp.sec = int(sec)
    msg.header.stamp.nanosec = int(nanosec)
    msg.header.frame_id = frame_id
    
    msg.orientation.x = duro_data['orientation_q'][0]
    msg.orientation.y = duro_data['orientation_q'][1]
    msg.orientation.z = duro_data['orientation_q'][2]
    msg.orientation.w = duro_data['orientation_q'][3]
    msg.orientation_covariance[0] = 0.01 
    msg.orientation_covariance[4] = 0.01
    msg.orientation_covariance[8] = 0.01
    
    msg.angular_velocity.x = livox_data['angular_velocity'][0]
    msg.angular_velocity.y = livox_data['angular_velocity'][1]
    msg.angular_velocity.z = livox_data['angular_velocity'][2]
    msg.angular_velocity_covariance[0] = 0.01 
    msg.angular_velocity_covariance[4] = 0.01
    msg.angular_velocity_covariance[8] = 0.01

    msg.linear_acceleration.x = livox_data['linear_acceleration'][0]
    msg.linear_acceleration.y = livox_data['linear_acceleration'][1]
    msg.linear_acceleration.z = livox_data['linear_acceleration'][2]
    msg.linear_acceleration_covariance[0] = 0.01
    msg.linear_acceleration_covariance[4] = 0.01
    msg.linear_acceleration_covariance[8] = 0.01
            
    return msg

CHANNEL_PARSERS = {
    "avia_points": parse_livox_point_packet,
    "avia_imu": parse_livox_imu_data, 
    "duro_gps_orient_eule": parse_duro_orient_euler 
}

def create_ros2_bag(bin_file_path, output_bag_dir):
    if os.path.exists(output_bag_dir):
        print(f"Output directory {output_bag_dir} already exists. Removing it.")
        try:
            shutil.rmtree(output_bag_dir) 
        except OSError as e:
            print(f"Error removing existing directory {output_bag_dir}: {e}. Please check permissions or remove manually.")
            return 
    
    writer = rosbag2_py.SequentialWriter()
    storage_options = rosbag2_py.StorageOptions(uri=output_bag_dir, storage_id='sqlite3')
    converter_options = rosbag2_py.ConverterOptions(
        input_serialization_format='cdr',
        output_serialization_format='cdr'
    )
    
    try:
        writer.open(storage_options, converter_options)
    except Exception as e: 
        print(f"RuntimeError opening bag writer for {output_bag_dir}: {e}")
        return

    topic_metadata_map = {
        LIDAR_TOPIC: rosbag2_py.TopicMetadata(name=LIDAR_TOPIC, type='sensor_msgs/msg/PointCloud2', serialization_format='cdr'),
        IMU_TOPIC: rosbag2_py.TopicMetadata(name=IMU_TOPIC, type='sensor_msgs/msg/Imu', serialization_format='cdr')
    }
    writer.create_topic(topic_metadata_map[LIDAR_TOPIC])
    writer.create_topic(topic_metadata_map[IMU_TOPIC])
    print(f"Registered topics: {LIDAR_TOPIC}, {IMU_TOPIC} for bag {output_bag_dir}")

    message_count_total = 0
    imu_combined_count = 0
    channel_counts = Counter()
    print(f"Processing .bin file: {bin_file_path} into {output_bag_dir}")

    duro_orientation_buffer.clear() # Clear for this bag file

    bin_envelope_timestamp_ms = 0 

    with open(bin_file_path, 'rb') as log_file:
        while True:
            try:
                timestamp_bytes = log_file.read(8)
                if not timestamp_bytes: break
                bin_envelope_timestamp_ms = int.from_bytes(timestamp_bytes, byteorder='big')

                channel_len_byte = log_file.read(1)
                if not channel_len_byte: break
                channel_len = int.from_bytes(channel_len_byte, byteorder='big')

                channel_name_bytes = log_file.read(channel_len)
                if not channel_name_bytes: break
                channel_name = channel_name_bytes.decode('utf-8', errors='replace')
                channel_counts[channel_name] += 1 

                msg_len_bytes = log_file.read(4)
                if not msg_len_bytes: break
                msg_len = int.from_bytes(msg_len_bytes, byteorder='big')

                raw_msg_data = log_file.read(msg_len)
                if len(raw_msg_data) != msg_len: 
                    print(f"Warning: Truncated message for channel {channel_name} at end of file {bin_file_path}. Expected {msg_len}, got {len(raw_msg_data)}.")
                    break 
            except EOFError:
                break
            except Exception as e:
                print(f"Error reading from .bin file {bin_file_path}: {e}")
                break

            if channel_name not in CHANNEL_PARSERS:
                continue
            
            parser_func = CHANNEL_PARSERS[channel_name]
            parsed_data = parser_func(raw_msg_data, bin_envelope_timestamp_ms)

            if not parsed_data:
                continue

            if channel_name == "avia_points":
                ros_msg = create_pointcloud2_msg(parsed_data, LIDAR_FRAME_ID)
                if ros_msg:
                    ros_timestamp_ns = (parsed_data['sec'] * 1_000_000_000) + parsed_data['nanosec']
                    writer.write(LIDAR_TOPIC, serialize_message(ros_msg), ros_timestamp_ns)
                    message_count_total += 1
            
            elif channel_name == "duro_gps_orient_eule":
                duro_orientation_buffer.append(parsed_data) 
            
            elif channel_name == "avia_imu":
                combined_count_this_call = try_combine_imu_data_new(parsed_data, writer)
                if combined_count_this_call > 0:
                    imu_combined_count += combined_count_this_call
                    message_count_total += combined_count_this_call
            
            if message_count_total > 0 and message_count_total % 200 == 0 :
                print(f"[{os.path.basename(output_bag_dir)}] Wrote {message_count_total} ROS msgs (IMU combined: {imu_combined_count})...")
        
        print(f"[{os.path.basename(output_bag_dir)}] Reached end of .bin file. Channel distribution in {os.path.basename(bin_file_path)}:")
        for name, count_val in channel_counts.items():
            print(f"  {name}: {count_val} messages")
        
        print(f"[{os.path.basename(output_bag_dir)}] Duro orientation buffer remaining: {len(duro_orientation_buffer)}")


    del writer 
    print(f"Finished processing {bin_file_path}. Wrote {message_count_total} ROS messages to {output_bag_dir} (Total IMU combined: {imu_combined_count})")


def process_bin_path(input_path, output_base_dir):
    if os.path.isfile(input_path) and input_path.endswith(".bin"):
        print(f"Converting single .bin file: {input_path}")
        base_name = os.path.splitext(os.path.basename(input_path))[0]
        output_bag_dir = os.path.join(output_base_dir, base_name + "_bag")
        create_ros2_bag(input_path, output_bag_dir) 
    elif os.path.isdir(input_path):
        print(f"Converting .bin files in directory: {input_path}")
        bin_files = sorted(glob.glob(os.path.join(input_path, '*.bin')))
        if not bin_files:
            print(f"No .bin files found in {input_path}")
            return
        for bin_file in bin_files:
            base_name = os.path.splitext(os.path.basename(bin_file))[0]
            output_bag_dir = os.path.join(output_base_dir, base_name + "_bag")
            create_ros2_bag(bin_file, output_bag_dir) 
    else:
        print(f"Invalid path (not a .bin file or directory): {input_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python your_script_name.py <input_bin_file_or_dir> [output_base_directory]")
        sys.exit(1)

    input_path_arg = sys.argv[1]
    output_base_dir_arg = sys.argv[2] if len(sys.argv) > 2 else "ros2_bags_output"

    if not os.path.exists(output_base_dir_arg):
        os.makedirs(output_base_dir_arg)
    try:
        process_bin_path(input_path_arg, output_base_dir_arg)
    finally:
        pass
    print("Conversion process complete.")