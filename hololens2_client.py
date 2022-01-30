import socket
import struct
import abc
import threading
from datetime import datetime, timedelta
from collections import namedtuple, deque
from enum import Enum
import numpy as np
import cv2
import time
import select
import os
import _thread
import json
import qoi
import lz4.block
import yaml

import rospy
from geometry_msgs.msg import PoseStamped
from geometry_msgs.msg import PoseArray


###############################################################################
# USER ADJUSTABLE PARAMETERS

HOST = '192.168.1.155'
OUTPUT_PATH = './images'

VIDEO_STREAM_PORT = 23940
AHAT_STREAM_PORT = 23941
LEFT_FRONT_STREAM_PORT = 23942
RIGHT_FRONT_STREAM_PORT = 23943

VIDEO_UDP_PORT = 21110
AHAT_UDP_PORT = 21111
LEFT_FRONT_UDP_PORT = 21112
RIGHT_FRONT_UDP_PORT = 21113

VIDEO_REQUEST_TIMEOUT = .1
AHAT_REQUEST_TIMEOUT = .1
VLC_REQUEST_TIMEOUT = .1

FPS_PRINT_INTERVAL = 10

SOCKET_RESTART_TIMEOUT = 3



###############################################################################

# Global program variables
should_save_next_images = False
img_save_counter = 0
should_restart_sockets = False


hmd_lock = threading.Lock()
inst_lock = threading.Lock()
spheres_lock = threading.Lock()

hmd_msg = ""
inst_msg = ""
spheres_msg = ""


###############################################################################

# ROS Stuff

rospy.init_node('listener', anonymous=True)




def save_ros_msg(msg, path):
   # Save a ROS message to JSON format
    y = yaml.load(str(msg))

    with open( path, "w") as f:
        json.dump(y, f, indent=4)
    

def save_all_ros_msg():
    with hmd_lock:
        save_ros_msg(hmd_msg, os.path.join(run_save_path, f"HMD_POSE_{img_save_counter:05}.json"))

    with inst_lock: 
        save_ros_msg(inst_msg, os.path.join(run_save_path, f"INST_POSE_{img_save_counter:05}.json"))

    with spheres_lock:
        save_ros_msg(spheres_msg, os.path.join(run_save_path, f"SPHERES_POSE_{img_save_counter:05}.json"))


def hmd_callback(msg):
    global run_save_path, img_save_counter, hmd_msg, hmd_lock
    with hmd_lock:
        hmd_msg = msg

def hmd_listener():
    # rospy.init_node('hmd_listener', anonymous=True)

    rospy.Subscriber("/atracsys/Hololens/measured_cp", PoseStamped, hmd_callback)

    # spin() simply keeps python from exiting until this node is stopped
    rospy.spin()


def inst_callback(msg):
    global run_save_path, img_save_counter, inst_msg, inst_lock
    with inst_lock:
        inst_msg = msg

def inst_listener():
    # rospy.init_node('inst_listener', anonymous=True)

    rospy.Subscriber("/atracsys/Instrument/measured_cp", PoseStamped, inst_callback)

    # spin() simply keeps python from exiting until this node is stopped
    rospy.spin()


def spheres_callback(msg):
    global run_save_path, img_save_counter, spheres_msg, spheres_lock
    with spheres_lock:
        spheres_msg = msg


def spheres_listener():
    # rospy.init_node('spheres_listener', anonymous=True)

    rospy.Subscriber("/atracsys/Controller/measured_cp_array", PoseArray, spheres_callback)

    # spin() simply keeps python from exiting until this node is stopped
    rospy.spin()

################################################################################################

np.warnings.filterwarnings('ignore')

# Definitions
# Protocol Header Format
# see https://docs.python.org/2/library/struct.html#format-characters
VIDEO_STREAM_HEADER_FORMAT = "@qIIIII18f"

VIDEO_FRAME_STREAM_HEADER = namedtuple(
    'SensorFrameStreamHeader',
    'Timestamp ImageWidth ImageHeight PixelStride RowStride BufLen fx fy '
    'PVtoWorldtransformM11 PVtoWorldtransformM12 PVtoWorldtransformM13 PVtoWorldtransformM14 '
    'PVtoWorldtransformM21 PVtoWorldtransformM22 PVtoWorldtransformM23 PVtoWorldtransformM24 '
    'PVtoWorldtransformM31 PVtoWorldtransformM32 PVtoWorldtransformM33 PVtoWorldtransformM34 '
    'PVtoWorldtransformM41 PVtoWorldtransformM42 PVtoWorldtransformM43 PVtoWorldtransformM44 '
)

RM_STREAM_HEADER_FORMAT = "@qIIIII16f"

RM_FRAME_STREAM_HEADER = namedtuple(
    'SensorFrameStreamHeader',
    'Timestamp ImageWidth ImageHeight PixelStride RowStride BufLen '
    'rig2worldTransformM11 rig2worldTransformM12 rig2worldTransformM13 rig2worldTransformM14 '
    'rig2worldTransformM21 rig2worldTransformM22 rig2worldTransformM23 rig2worldTransformM24 '
    'rig2worldTransformM31 rig2worldTransformM32 rig2worldTransformM33 rig2worldTransformM34 '
    'rig2worldTransformM41 rig2worldTransformM42 rig2worldTransformM43 rig2worldTransformM44 '
)


class FrameReceiverThread(threading.Thread):
    def __init__(self, host, port, udp_port, header_format, header_data, req_resend_timeout, sensor_name="NoSensorName"):
        super(FrameReceiverThread, self).__init__()
        self.header_size = struct.calcsize(header_format)
        self.header_format = header_format
        self.header_data = header_data
        self.host = host
        self.port = port
        self.udp_port = udp_port

        self.latest_frame = None
        self.latest_header = None
        self.socket = None
        self.udp_socket = None

        self.req_resend_timeout = req_resend_timeout
        self.sensor_name = sensor_name

        self.lock = threading.Lock()

        self.should_stop = False


    # def get_data_from_socket(self, debug=False):
    #     # read image header
    #     reply = self.recvall(self.header_size, timeout=self.req_resend_timeout)

    #     if reply is None:
    #         # reply is None if self.recvall timed out
    #         return None

    #     data = struct.unpack(self.header_format, reply)
    #     header = self.header_data(*data)
    #     image_size_bytes = header.ImageHeight * header.RowStride

    #     # read the image
    #     image_data = self.recvall(image_size_bytes, timeout=None)


    #     return header, image_data

    def recvall(self, size, timeout=None):
        received_any = False # set to True if at least 1 byte received
        msg = bytearray()    #       

        # Check with select() for data on the socket.
        # If select times out
        # before any data is received None is returned. If some data is received
        # at all, then it is assumed that in total `size` bytes are coming. This
        # will keep reading until the expected number of bytes come in
        while len(msg) < size:
            if (timeout is None) or (received_any):
                # if there is a timeout or some bytes have been read, select
                # will wait indefinitely since more bytes should be coming
                read_sockets, _, _ = select.select([self.socket], [], [])

            else:
                # if there is a timeout, and there is nothing to read from
                # select(), return none
                read_sockets, _, _ = select.select([self.socket], [], [], timeout)

                if (len(read_sockets) == 0) and (not received_any):
                    
                    return None

            for s in read_sockets:
                if s == self.socket:
                    part = self.socket.recv(size - len(msg)) # blocks until data

                    if part != '':
                        received_any = True

                        msg += part
                    else:
                        print("ERROR: empty recv part")
                        # interrupts main thread, and causes the program to exit
                        _thread.interrupt_main()

        return msg

    def start_socket(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)
        self.socket.connect((self.host, self.port))

        print('INFO: Socket connected to ' + self.host + ' on port ' + str(self.port))
        
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
        print('INFO: UDP Socket created')

    def start_listen(self):
        self.should_stop = False
        t = threading.Thread(target=self.listen)
        t.daemon = True
        t.start()

        self.listen_thread = t

    def stop(self):
        self.should_stop = True
        self.listen_thread.join()
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()

    def req_next_frame(self):
        # UDP message includes a newline at the end so that netcat can also be used
        # easily for debugging
        # 
        # send two packets for redundancy     
        self.udp_socket.sendto(bytes("1\n", "utf-8"), (self.host, self.udp_port))


    @abc.abstractmethod
    def listen(self):
        return

    @abc.abstractmethod
    def get_mat_from_header(self, header):
        return


class VideoReceiverThread(FrameReceiverThread):
    def __init__(self, host):
        super().__init__(host, VIDEO_STREAM_PORT, VIDEO_UDP_PORT, VIDEO_STREAM_HEADER_FORMAT,
                         VIDEO_FRAME_STREAM_HEADER, VIDEO_REQUEST_TIMEOUT, sensor_name="VIDEO")

    def listen(self):
        count = 0
        start = time.time()

        
        while True:
            self.req_next_frame()
            ret = self.get_data_from_socket()

            if ret is not None:
                # Mutex used so that the header and latest frame always match
                # when read by the other thread
                with self.lock:
                    self.latest_header, image_data = ret
                    self.latest_frame = np.frombuffer(image_data, dtype=np.uint8).reshape((self.latest_header.ImageHeight,
                                                                                        self.latest_header.ImageWidth,
                                                                                        self.latest_header.PixelStride))                                                          
                end = time.time()
                count += 1
                if (end-start) > FPS_PRINT_INTERVAL:
                    # print(self.sensor_name, "receive FPS: ", count / (end-start))
                    count = 0
                    start = time.time()

            else:
                self.req_next_frame()


            if self.should_stop:
                return

    def get_mat_from_header(self, header):
        pv_to_world_transform = np.array(header[7:24]).reshape((4, 4)).T
        return pv_to_world_transform


    def get_data_from_socket(self, debug=False):
        # read image header
        reply = self.recvall(self.header_size, timeout=self.req_resend_timeout)


        if reply is None:
            # reply is None if self.recvall timed out
            # print(self.sensor_name, ": Header Timeout")
            return None

        data = struct.unpack(self.header_format, reply)
        header = self.header_data(*data)
        image_size_bytes = header.ImageHeight * header.RowStride

        # read the image
        image_data = self.recvall(header.BufLen, timeout=SOCKET_RESTART_TIMEOUT)

        
        if image_data is None:
            print(self.sensor_name, ": Image Timeout")

            global should_restart_sockets
            should_restart_sockets = True
            return None


        max_uncompressed_size = 1952*1100 * 2

        pass_1 = lz4.block.decompress(image_data, uncompressed_size=max_uncompressed_size)

        qoi_image = lz4.block.decompress(pass_1, uncompressed_size=max_uncompressed_size)
        
        # bgr_decoded = lz4.block.decompress(image_data, uncompressed_size=max_uncompressed_size)

        bgr_decoded = qoi.decode(qoi_image)


        # print("BufLen_PV", header.BufLen)
        # print("bgr_decoded shape", bgr_decoded.shape)


        return header, bgr_decoded


class AhatReceiverThread(FrameReceiverThread):
    def __init__(self, host):
        super().__init__(host,
                         AHAT_STREAM_PORT, AHAT_UDP_PORT, RM_STREAM_HEADER_FORMAT, RM_FRAME_STREAM_HEADER,
                         AHAT_REQUEST_TIMEOUT, sensor_name="AHAT")

        self.latest_depth_frame = None
        self.latest_ab_frame = None

    def listen(self):
        count = 0
        start = time.time()


        while True:
            self.req_next_frame()

            ret = self.get_data_from_socket()

            if ret is not None:
                # Mutex used so that the header and latest frame always match
                # when read by the other thread
                with self.lock:
                    self.latest_header, depth_data, ab_data = ret
                    self.latest_depth_frame = np.frombuffer(depth_data, dtype=np.uint16).reshape((self.latest_header.ImageHeight,
                                                                                    self.latest_header.ImageWidth))
                    self.latest_ab_frame = np.frombuffer(ab_data, dtype=np.uint16).reshape((self.latest_header.ImageHeight,
                                                                                    self.latest_header.ImageWidth))


                end = time.time()
                count += 1
                if (end-start) > FPS_PRINT_INTERVAL:
                    # print(self.sensor_name, "receive FPS: ", count / (end-start))
                    count = 0
                    start = time.time()
                                                                                        
            else:
                self.req_next_frame()


            if self.should_stop:
                return

    def get_mat_from_header(self, header):
        rig_to_world_transform = np.array(header[5:22]).reshape((4, 4)).T
        return rig_to_world_transform



    def get_data_from_socket(self, debug=False):
        # THIS METHOD IS OVERRIDING THE FrameReceiverThread method

        # read image header
        reply = self.recvall(self.header_size, timeout=self.req_resend_timeout)

        if reply is None:
            # reply is None if self.recvall timed out
            # print(self.sensor_name, ": Header Timeout")

            return None

        data = struct.unpack(self.header_format, reply)
        header = self.header_data(*data)
        image_size_bytes = header.ImageHeight * header.RowStride

        # read the image
        image_data = self.recvall(header.BufLen, timeout=SOCKET_RESTART_TIMEOUT)
        
        
        if image_data is None:
            print(self.sensor_name, ": Image Timeout")

            global should_restart_sockets
            should_restart_sockets = True
            return None

        # print("BufLen", self.sensor_name, header.BufLen)

        max_uncompressed_size = 512*512*4 * 2
        pass_1 = lz4.block.decompress(image_data, uncompressed_size=max_uncompressed_size)
        qoi_image = lz4.block.decompress(pass_1, uncompressed_size=max_uncompressed_size)



        depth_combined_decoded = bytearray(qoi.decode(qoi_image))


        depth_image = depth_combined_decoded[:image_size_bytes]
        ab_image = depth_combined_decoded[image_size_bytes:]



        return header, depth_image, ab_image



class VLC_ReceiverThread(FrameReceiverThread):
    def __init__(self, host, camera="LF"):

        if camera == "LF":
            super().__init__(host,
                         LEFT_FRONT_STREAM_PORT, LEFT_FRONT_UDP_PORT, RM_STREAM_HEADER_FORMAT,
                         RM_FRAME_STREAM_HEADER, VLC_REQUEST_TIMEOUT, sensor_name="VLC_LF")
                         
        elif camera == "RF":
            super().__init__(host,
                         RIGHT_FRONT_STREAM_PORT, RIGHT_FRONT_UDP_PORT, RM_STREAM_HEADER_FORMAT,
                         RM_FRAME_STREAM_HEADER, VLC_REQUEST_TIMEOUT, sensor_name="VLC_RF")

        else:
            print("Only LF and RF cameras implemented")


    def listen(self):
        count = 0
        start = time.time()


        while True:
            self.req_next_frame()
            ret = self.get_data_from_socket()

            if ret is not None:
                # Mutex used so that the header and latest frame always match
                # when read by the other thread
                with self.lock:
                    self.latest_header, image_data = ret
                    self.latest_frame = np.frombuffer(image_data, dtype=np.uint8).reshape((self.latest_header.ImageHeight,
                                                                                            self.latest_header.ImageWidth))
                end = time.time()
                count += 1
                if (end-start) > FPS_PRINT_INTERVAL:
                    # print(self.sensor_name, "receive FPS: ", count / (end-start))
                    count = 0
                    start = time.time()
            
            else:
                self.req_next_frame()


            if self.should_stop:
                return

    def get_mat_from_header(self, header):
        rig_to_world_transform = np.array(header[5:22]).reshape((4, 4)).T
        return rig_to_world_transform


    def get_data_from_socket(self, debug=False):
        # read image header
        reply = self.recvall(self.header_size, timeout=self.req_resend_timeout)

        if reply is None:
            # reply is None if self.recvall timed out
            # print(self.sensor_name, ": Header Timeout")

            return None

        data = struct.unpack(self.header_format, reply)
        header = self.header_data(*data)
        image_size_bytes = header.ImageHeight * header.RowStride

        # read the image
        image_data = self.recvall(header.BufLen, timeout=SOCKET_RESTART_TIMEOUT)

        if image_data is None:
            print(self.sensor_name, ": Image Timeout")

            global should_restart_sockets
            should_restart_sockets = True
            return None
        

        max_uncompressed_size = 640*480 * 2

        pass_1 = lz4.block.decompress(image_data, uncompressed_size=max_uncompressed_size)
        qoi_image = lz4.block.decompress(pass_1, uncompressed_size=max_uncompressed_size)


        vlc_decoded = qoi.decode(qoi_image)


        # print("BufLen", self.sensor_name, header.BufLen)
        # print("vlc_decoded shape", vlc_decoded.shape, vlc_decoded.shape[1] * vlc_decoded.shape[2] * vlc_decoded.shape[0])

        return header, vlc_decoded


def create_unique_output_folder():
    if not os.path.isdir(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH)

    counter = 1
    subfolder_path = os.path.join(OUTPUT_PATH,f"run_{counter}")
    while os.path.isdir(subfolder_path):
        subfolder_path = os.path.join(OUTPUT_PATH,f"run_{counter}")
        counter += 1

    os.makedirs(subfolder_path)


    return subfolder_path


def handle_mouse(event, x, y, flags, param):
    if event == cv2.EVENT_LBUTTONDOWN:
        global img_save_counter
        img_save_counter = max( img_save_counter - 1, 0)
        print("Next capture will overwrite capture", img_save_counter)

    if event == cv2.EVENT_RBUTTONDOWN:
        global should_save_next_images
        should_save_next_images = True


def _connect_to_hololens(receiver_list):
    print("Connecting to HoloLens2...")

    for receiver in receiver_list:
        receiver.start_socket()

    for receiver in receiver_list:
        receiver.start_listen() 

def connect_to_hololens(receiver_list):
    # This is only done in a thread so that ctrl-c can end the program otherwise
    # it will get stuck waiting to connect to the hololens
    t = threading.Thread(target=_connect_to_hololens, args=( receiver_list ,))
    t.daemon = True
    t.start()

    while True:
        t.join(0.5)
        if not t.is_alive():
            break  


def _restart_sockets(receiver_list):
    print("Attempting to restart sockets")
    for receiver in receiver_list:
        receiver.stop()            

    for receiver in receiver_list:
        receiver.start_socket()

    for receiver in receiver_list:
        receiver.start_listen()

    print("Reconnected to HoloLens2...")


def restart_sockets(receiver_list):
    # This is only done in a thread so that ctrl-c can end the program otherwise
    # it will get stuck waiting
    t = threading.Thread(target=_restart_sockets, args=( receiver_list ,))
    t.daemon = True
    t.start()

    while True:
        t.join(0.5)
        if not t.is_alive():
            break   


def save_all_data(run_save_path, receivers_list, img_save_counter):
    
    save_all_ros_msg()

    for receiver in receivers_list:
        header = None
        image = None



        if receiver.sensor_name == "AHAT":
            with receiver.lock:
                header = receiver.latest_header # namedtuple is immutable so this is a copy
                depth_image = receiver.latest_depth_frame.copy() # want to copy to release the lock quickly
                ab_image = receiver.latest_ab_frame.copy() # want to copy to release the lock quickly
            
            cv2.imwrite(os.path.join(run_save_path, f"AHAT_DEPTH_{img_save_counter:05}.png"), depth_image)
            cv2.imwrite(os.path.join(run_save_path, f"AHAT_AB_{img_save_counter:05}.png"), ab_image)

            with open( os.path.join(run_save_path, f"{receiver.sensor_name}_{img_save_counter:05}.json"), "w") as f:
                json.dump(header._asdict(), f, indent=4)

        else:
            with receiver.lock:
                header = receiver.latest_header # namedtuple is immutable so this is a copy
                image = receiver.latest_frame.copy() # want to copy to release the lock quickly
            
            if receiver.sensor_name == "VLC_LF":
                image = cv2.rotate(front_left_receiver.latest_frame, cv2.ROTATE_90_CLOCKWISE)
                print(header)

            elif receiver.sensor_name == "VLC_RF":
                image = cv2.rotate(front_right_receiver.latest_frame, cv2.ROTATE_90_COUNTERCLOCKWISE)
            

            cv2.imwrite(os.path.join(run_save_path, f"{receiver.sensor_name}_{img_save_counter:05}.png"), image)

            with open( os.path.join(run_save_path, f"{receiver.sensor_name}_{img_save_counter:05}.json"), "w") as f:
                json.dump(header._asdict(), f, indent=4)



if __name__ == '__main__':

    # ROS SUBSCRIBERS
    sub1 = threading.Thread(target=hmd_listener)
    sub1.daemon = True
    sub1.start()

    sub2 = threading.Thread(target=inst_listener)
    sub2.daemon = True
    sub2.start()

    sub3 = threading.Thread(target=spheres_listener)
    sub3.daemon = True
    sub3.start()




    video_receiver = VideoReceiverThread(HOST)
    ahat_receiver = AhatReceiverThread(HOST)
    front_left_receiver = VLC_ReceiverThread(HOST, camera="LF")
    front_right_receiver = VLC_ReceiverThread(HOST, camera="RF")

    receivers_list = [video_receiver, ahat_receiver, front_left_receiver, front_right_receiver]

    connect_to_hololens(receivers_list)

    run_save_path = create_unique_output_folder()

    cv2.namedWindow('Photo Video Camera Stream', cv2.WINDOW_AUTOSIZE | cv2.WINDOW_GUI_NORMAL )
    cv2.setMouseCallback('Photo Video Camera Stream', handle_mouse)

    cv2.namedWindow('Depth Camera Depth Stream', cv2.WINDOW_AUTOSIZE | cv2.WINDOW_GUI_NORMAL)
    cv2.setMouseCallback('Depth Camera Depth Stream', handle_mouse)

    cv2.namedWindow('Depth Camera Ab Stream', cv2.WINDOW_AUTOSIZE | cv2.WINDOW_GUI_NORMAL)
    cv2.setMouseCallback('Depth Camera Ab Stream', handle_mouse)

    cv2.namedWindow('Front Left Camera Stream', cv2.WINDOW_AUTOSIZE | cv2.WINDOW_GUI_NORMAL)
    cv2.setMouseCallback('Front Left Camera Stream', handle_mouse)

    cv2.namedWindow('Front Right Camera Stream', cv2.WINDOW_AUTOSIZE | cv2.WINDOW_GUI_NORMAL)
    cv2.setMouseCallback('Front Right Camera Stream', handle_mouse)



    try:
        while True:
            have_video_frame = np.any(video_receiver.latest_frame)
            if have_video_frame:
                cv2.imshow('Photo Video Camera Stream', video_receiver.latest_frame)

            have_ahat_frame = np.any(ahat_receiver.latest_depth_frame) and np.any(ahat_receiver.latest_ab_frame)
            if have_ahat_frame:
                cv2.imshow('Depth Camera Depth Stream', ahat_receiver.latest_depth_frame)
                cv2.imshow('Depth Camera Ab Stream', ahat_receiver.latest_ab_frame)

            have_front_left_frame = np.any(front_left_receiver.latest_frame)
            if have_front_left_frame:
                rotated_image = cv2.rotate(front_left_receiver.latest_frame, cv2.ROTATE_90_CLOCKWISE)
                cv2.imshow('Front Left Camera Stream', rotated_image)

            have_front_right_frame = np.any(front_right_receiver.latest_frame) 
            if have_front_right_frame:
                rotated_image = cv2.rotate(front_right_receiver.latest_frame, cv2.ROTATE_90_COUNTERCLOCKWISE)  
                cv2.imshow('Front Right Camera Stream', rotated_image)

            key = cv2.waitKey(1) & 0xFF
            
            if key == ord('q'):
                break
            elif key == ord(' '):
                should_save_next_images = True
            elif key == ord(','):
                img_save_counter = max( img_save_counter - 1, 0)
                print("Next capture will overwrite capture", img_save_counter)
            elif key == ord('.'):
                img_save_counter += 1
                print("Next capture will overwrite capture", img_save_counter)
            elif key == ord('r'):
                should_restart_sockets = True
                restart_sockets(receivers_list)
            
            if should_save_next_images:
                should_save_next_images = False

                if (have_video_frame and have_ahat_frame and have_front_left_frame and have_front_right_frame):
                    save_all_data(run_save_path, receivers_list, img_save_counter)
                    
                    print("Saved capture", img_save_counter)

                    img_save_counter += 1

                else:
                    print("Cannot save, missing a sensor frame")

            if should_restart_sockets:
                restart_sockets(receivers_list)
                should_restart_sockets = False



    except KeyboardInterrupt:
        pass

    finally:
        for receiver in receivers_list:
            receiver.stop()            

        print("closed sockets gracefully")