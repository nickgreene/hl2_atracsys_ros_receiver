import threading
import cv2
import os
import json
import yaml

import rospy
from geometry_msgs.msg import PoseStamped
from geometry_msgs.msg import PoseArray



OUTPUT_PATH = './data'

###############################################################################

# Global variables
img_save_counter = 0

hmd_lock = threading.Lock()
inst_lock = threading.Lock()
spheres_lock = threading.Lock()

hmd_msg = ""
inst_msg = ""
spheres_msg = ""

###############################################################################

# create a callback and listener (and lock) for each ros topic

def hmd_callback(msg):
    global run_save_path, img_save_counter, hmd_msg, hmd_lock
    with hmd_lock:
        hmd_msg = msg

def hmd_listener():
    rospy.Subscriber("/atracsys/Hololens/measured_cp", PoseStamped, hmd_callback)
    rospy.spin()

def inst_callback(msg):
    global run_save_path, img_save_counter, inst_msg, inst_lock
    with inst_lock:
        inst_msg = msg

def inst_listener():
    rospy.Subscriber("/atracsys/Instrument/measured_cp", PoseStamped, inst_callback)
    rospy.spin()


def spheres_callback(msg):
    global run_save_path, img_save_counter, spheres_msg, spheres_lock
    with spheres_lock:
        spheres_msg = msg

def spheres_listener():
    rospy.Subscriber("/atracsys/Controller/measured_cp_array", PoseArray, spheres_callback)
    rospy.spin()


def save_ros_msg_as_json(msg, path):
   # Save a string ROS message to JSON format
    y = yaml.load(str(msg))

    with open( path, "w") as f:
        json.dump(y, f, indent=4)
    

def save_all_ros_msg():
    global img_save_counter

    with hmd_lock:
        save_ros_msg_as_json(hmd_msg, os.path.join(run_save_path, f"HMD_POSE_{img_save_counter:05}.json"))

    with inst_lock: 
        save_ros_msg_as_json(inst_msg, os.path.join(run_save_path, f"INST_POSE_{img_save_counter:05}.json"))
           
    with spheres_lock: 
        save_ros_msg_as_json(spheres_msg, os.path.join(run_save_path, f"SPHERES_POSE_{img_save_counter:05}.json"))


    print("Saved capture ", img_save_counter)
    img_save_counter += 1

##############################################################################


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
        save_all_ros_msg()


if __name__ == '__main__':
    rospy.init_node('atracsys_listener', anonymous=True)

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


    run_save_path = create_unique_output_folder()

    cv2.namedWindow('dummy window', cv2.WINDOW_AUTOSIZE | cv2.WINDOW_GUI_NORMAL )
    cv2.setMouseCallback('dummy window', handle_mouse)

    dummy_image = cv2.imread('dummy_image.jpeg')

    cv2.imshow('dummy window', dummy_image)

    while True:
        try:
            key = cv2.waitKey(100) & 0xFF
            
            if key == ord('q'):
                break
            elif key == ord(' '):
                save_all_ros_msg()
            elif key == ord(','):
                img_save_counter = max( img_save_counter - 1, 0)
                print("Next capture will overwrite capture", img_save_counter)
            elif key == ord('.'):
                img_save_counter += 1
                print("Next capture will overwrite capture", img_save_counter)


            elif key == 99: # ctrl C on my ubuntu
                break

        except KeyboardInterrupt:
            print("Exiting")
            break