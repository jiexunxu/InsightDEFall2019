# Send an email notification to user which contains the history of parameters used by that email, provide a download link,and save the user's request into a database

import smtplib
from email.message import EmailMessage
import datetime


def email_and_log(
    output_foldername,
    connection,
    user_email,
    user_selection,
    user_param,
    user_labels,
    is_large_scale,
):
    # Email message for small scale processing, user can download processed images and metadata directly
    def small_dataset_message():
        msg = "Hello, your request is completed. You can download your images at: \n"
        msg += (
            "http://jiexunxu-open-image-dataset.s3.amazonaws.com/output_data/"
            + output_foldername
            + "result.zip\n\n"
        )
        msg += (
            "The bounding boxes meta-data for your augmented images is available at: \n"
        )
        msg += (
            "http://jiexunxu-open-image-dataset.s3.amazonaws.com/output_data/"
            + output_foldername
            + "selected-train-annotations-bbox.csv"
        )
        return msg

    # Email message for large scale processing, user need aws cli to sync their data
    def large_dataset_message():
        msg = "Hello, your request is completed. Due to the amount of images you requested to process, "
        msg += "please install aws-cli to sync your processed images and meta-data with the following command:\n\n"
        msg += (
            "aws s3 sync s3://jiexunxu-open-image-dataset/output_data/"
            + output_foldername
            + " ."
        )
        return msg

    # Get the current email's historical parameter choices
    def get_user_history():
        cursor = connection.cursor()
        cursor.execute(
            "SELECT * FROM user_history WHERE email='"
            + user_email
            + "' ORDER BY date_time DESC"
        )
        histories = cursor.fetchall()
        msg = "\n\n\nFor reproducible machine learning purposes, following user parameter combos have been submitted from this email address before:"
        for row in histories:
            msg += "\n" + row[1] + " " + row[2]
        return msg

    # Email the current user
    def email():
        msg = EmailMessage()
        if is_large_scale:
            msg.set_content(large_dataset_message() + get_user_history())
        else:
            msg.set_content(small_dataset_message() + get_user_history())
        msg["Subject"] = "Your data is ready"
        msg["From"] = "service"
        msg["To"] = user_email
        s = smtplib.SMTP("localhost")
        s.send_message(msg)
        s.quit()

    # Record the current user's parameter choice into the database
    def log():
        history_str = (
            "Image size="
            + str(user_param[0])
            + ", Gaussian aperture="
            + str(user_param[1])
            + ", Gaussian sigma="
            + str(user_param[2])
            + ", Scale="
            + str(user_param[3])
            + ", Crop Xmin="
            + str(user_param[4])
            + ", Crop Xmax="
            + str(user_param[5])
            + ", Crop Ymin="
            + str(user_param[6])
            + ", Crop Ymax="
            + str(user_param[7])
            + ", Min #Objects="
            + str(user_selection[0])
            + ", Max #Objects="
            + str(user_selection[1])
        )
        if user_selection[2] == 1:
            history_str += ", Google verfied boxes only=True, Labels="
        else:
            history_str += ", Google verfied boxes only=False, Labels="
        for label in user_labels:
            history_str += label + ","
        history_str = history_str[:-1]
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO user_history (email, date_time, history) VALUES (%s, %s, %s)",
            (user_email, datetime.datetime.now(), history_str),
        )
        connection.commit()

    print("Job completed, record the user input and email user at " + user_email)
    log()
    email()
