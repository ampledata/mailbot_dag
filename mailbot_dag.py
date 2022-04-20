#!/usr/bin/env python3
"""
DAG for MailBot: OpenCV mailbox detector.
"""

import tempfile

from datetime import datetime
from io import BytesIO
from os import getenv
from random import randint
from urllib import request

import numpy as np
import cv2

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator


DEFAULT_SNAP_URL = getenv("SNAP_URL", "http://172.17.2.213/snap.jpeg")

start_date = datetime(2021, 1, 1)
schedule_interval = "*/5 * * * *"


def _faux_snap():
    src = "snap_no_mail_and_lights_daytime.jpg"
    with open(src, "rb") as src_fd:
        snap = src_fd.read()


def _create_blob_params():
    """Sets filtering paramaters for a SimpleBlobDetector instance."""
    params = cv2.SimpleBlobDetector_Params()

    # Set Area filtering parameters
    params.filterByArea = True
    params.minArea = 100

    # Set Circularity filtering parameters
    params.filterByCircularity = True
    params.minCircularity = 0.7

    # Set Convexity filtering parameters
    params.filterByConvexity = True
    params.minConvexity = 0.2

    # Set inertia filtering parameters
    params.filterByInertia = True
    params.minInertiaRatio = 0.01

    return params


def _get_snap(ti):
    """
    Gets a snapshot from our snapshot url. Xcom pushes a tmp_file name.
    """
    url: str = DEFAULT_SNAP_URL
    response = request.urlopen(url)

    tmp_fd, tmp_file = tempfile.mkstemp()
    with open(tmp_fd, "wb+") as handle:
        handle.write(response.read())

    ti.xcom_push(key="tmp_file", value=tmp_file)


def _find_keypoints(ti):
    """
    Finds the blob keypoints within a given image. Xcom pushes a list of
    keypoints.
    """
    tmp_file = ti.xcom_pull(task_ids="get_snap", key="tmp_file")

    img = cv2.imread(tmp_file)
    inverted = cv2.bitwise_not(img)
    aoi = inverted[350:720, 400:900]

    params = _create_blob_params()
    detector = cv2.SimpleBlobDetector_create(params)
    keypoints = detector.detect(aoi)

    ti.xcom_push(key="keypoints", value=keypoints)


def _choose_by_keypoints(ti):
    """
    Choses the next function based on the number of keypoints found in the
    image.
    """
    keypoints = ti.xcom_pull(task_ids="find_keypoints", key="keypoints")

    lkp = len(keypoints)
    if lkp == 0:
        return "mail_arrived"
    elif lkp == 4:
        return "mail_not_arrived"
    else:
        return "mail_not_detected"


def _mail_arrived():
    print("Mail Arrived!")


def _mail_not_arrived():
    print("Mail has not arrived.")


def _mail_not_detected():
    print("Can't detect mail.")


with DAG(
    "mailbot", start_date=start_date, schedule_interval=schedule_interval, catchup=False
) as dag:

    get_snap = PythonOperator(task_id="get_snap", python_callable=_get_snap)

    find_keypoints = PythonOperator(
        task_id="find_keypoints", python_callable=_find_keypoints
    )

    choose_by_keypoints = BranchPythonOperator(
        task_id="choose_by_keypoints", python_callable=_choose_by_keypoints
    )

    mail_arrived = PythonOperator(task_id="mail_arrived", python_callable=_mail_arrived)
    mail_not_arrived = PythonOperator(
        task_id="mail_not_arrived", python_callable=_mail_not_arrived
    )
    mail_not_detected = PythonOperator(
        task_id="mail_not_detected", python_callable=_mail_not_detected
    )

    # Graph:
    (
        get_snap
        >> find_keypoints
        >> choose_by_keypoints
        >> [mail_arrived, mail_not_arrived, mail_not_detected]
    )
