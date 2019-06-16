
from __future__ import division

from pipelineprocess.process import process
from pipelinetypes import p_array, p_image, KEY_MESSAGE

from src.models import *
from src.utils.utils import *
from src.utils.datasets import *

import os
import sys
import time
import datetime
import argparse

from PIL import Image

import torch
from torch.utils.data import DataLoader
from torchvision import datasets
from torch.autograd import Variable

import numpy as np
import cv2
import json

from pipelinesink.Writer.picklewriter import picklewriter


class yolov3(process):
    input = {"image": p_image}
    output = {"detections": p_array}

    def __init__(self, c_topic, p_topic, mapping, saveoutputflag, lastprocessflag, c_bootstrap_servers='localhost:9092', p_bootstrap_servers='localhost:9092'):
        super().__init__(mapping=mapping, saveoutputflag=saveoutputflag, lastprocessflag=lastprocessflag, c_topic=c_topic, p_topic=p_topic, c_bootstrap_servers=c_bootstrap_servers, p_bootstrap_servers=p_bootstrap_servers)

        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        # Set up model
        self.model = Darknet("pipelineprocess/highvision/objectdetection/detectors/yolov3/src/config/yolov3.cfg", img_size=416).to(device)

        weights_path = "pipelineprocess/highvision/objectdetection/detectors/yolov3/src/weights/yolov3.weights"
        class_path = "pipelineprocess/highvision/objectdetection/detectors/yolov3/src/data/coco.names"


        if weights_path.endswith(".weights"):
            print("weights")
            # Load darknet weights
            self.model.load_darknet_weights(weights_path)
        else:
            # Load checkpoint weights
            self.model.load_state_dict(torch.load(weights_path))

        self.model.eval()

        self.classes = load_classes(class_path)  # Extracts class labels from file

        self.Tensor = torch.cuda.FloatTensor if torch.cuda.is_available() else torch.FloatTensor
        self.input_size = 416

    @staticmethod
    def get_parer():
        parser = process.default_parser()

        return parser

    def preprocess_image(self, img):
        cv2_im = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        img = cv2_im.astype(np.float32, copy=False)

        img = cv2.resize(img, (self.input_size, self.input_size))

        img = img.reshape((1, 1, self.input_size, self.input_size))

        return img

    def letterbox_image(self, img, inp_dim):
        '''resize image with unchanged aspect ratio using padding'''
        img_w, img_h = img.shape[1], img.shape[0]
        w, h = inp_dim
        new_w = int(img_w * min(w / img_w, h / img_h))
        new_h = int(img_h * min(w / img_w, h / img_h))
        resized_image = cv2.resize(img, (new_w, new_h), interpolation=cv2.INTER_CUBIC)

        canvas = np.full((inp_dim[1], inp_dim[0], 3), 128)

        canvas[(h - new_h) // 2:(h - new_h) // 2 + new_h, (w - new_w) // 2:(w - new_w) // 2 + new_w, :] = resized_image

        return canvas


    def preprocess_image(self, img):
        orig_im = img

        img = (self.letterbox_image(orig_im, (self.input_size, self.input_size)))
        img_1 = img[:, :, ::-1].transpose((2, 0, 1)).copy()
        img_ = torch.from_numpy(img_1).float().div(255.0).unsqueeze(0)

        input_imgs = Variable(img_.type(self.Tensor))

        return input_imgs


    def process(self, inputmessage):
        message_dict = inputmessage

        image_str = message_dict["image"]
        x = np.fromstring(image_str["data"], dtype=image_str["dtype"])
        decoded = x.reshape(image_str["shape"])

        input_imgs = self.preprocess_image(decoded)

        with torch.no_grad():
            detections = self.model(input_imgs)
            detections = non_max_suppression(detections, 0.8, 0.4)

        msg_detections = []

        if detections is not None:
            # Rescale boxes to original image

            detections = rescale_boxes(detections[0], self.input_size, image_str["shape"][:2])
            print("Frameid: %s" % (message_dict["frameid"]))
            for x1, y1, x2, y2, conf, cls_conf, cls_pred in detections:

                bb = {'tl': (x1.item(), y1.item()), 'br': (x2.item(),y2.item())}
                msg_detections += [{'bb': bb, 'class': self.classes[int(cls_pred)], "confidence":cls_conf.item()}]
                print("Detected %s" % (self.classes[int(cls_pred)]) )

        message_dict["detections"] = msg_detections

        message = message_dict

        if self.saveoutputflag:
            self.saveoutput({"detections": msg_detections, "frameid": message_dict["frameid"], "time_stamp":message_dict["time_stamp"]})
        print({"detections": msg_detections, "frameid": message_dict["frameid"]})
        return str(message)

    def end_consuming(self):
        print("end consuming")
        if self.saveoutputflag:
            print("Closing")
            self.outwriter.close()

    def initializeoutputwriter(self):
        print("Initializing")
        self.outputfile = self.outputfilebase + ".py"
        self.outwriter = picklewriter(self.outputfile)
        self.dict_output_log = {"stage": self.stagename, "data":[{"type": "list_of_bb", "location": self.outputfile}]}


    def saveoutput(self, data):
        self.outwriter.write(data)

if __name__ == '__main__':
    parser = yolov3.get_parser()
    args = parser.parse_args()

    def converttojsonreadable(inputstring):
        inputstring = inputstring.replace(":","\":\"")
        inputstring = inputstring.replace("{", "{\"").replace("}", "\"}")
        inputstring = inputstring.replace(",", "\",\"")
        return inputstring

    args.mapping = json.loads(converttojsonreadable(args.mapping))
    p_yolo = yolov3(c_topic=args.c_topic, p_topic=args.p_topic, mapping=args.mapping, saveoutputflag=args.save_output, lastprocessflag=args.last_process, c_bootstrap_servers=args.c_bootstrap_servers, p_bootstrap_servers=args.p_bootstrap_servers)
    p_yolo.run()
