# Entry point of the flask app that exposes a web UI for users to submit request to the BOSA system
import sys

sys.path.insert(0, "./flask")
sys.path.insert(0, "./spark")
sys.path.insert(0, "./python")
from flask import Flask, render_template, flash, request
from wtforms import Form, TextField, TextAreaField, validators, StringField, SubmitField
import preprocess_query
import subprocess
import os

DEBUG = True
app = Flask(__name__)
app.config.from_object(__name__)
app.config["SECRET_KEY"] = os.urandom(12)

# All fields must be filled to submit a request
class ReusableForm(Form):
    email = TextField("", validators=[validators.required()])
    min_obj = TextField("", validators=[validators.required()])
    max_obj = TextField("", validators=[validators.required()])
    box_source = TextField("", validators=[validators.required()])
    labels = TextField("", validators=[validators.required()])
    image_size = TextField("", validators=[validators.required()])
    scale = TextField("", validators=[validators.required()])
    Xmin = TextField("", validators=[validators.required()])
    Xmax = TextField("", validators=[validators.required()])
    Ymin = TextField("", validators=[validators.required()])
    Ymax = TextField("", validators=[validators.required()])
    blur_size = TextField("", validators=[validators.required()])
    blur_sigma = TextField("", validators=[validators.required()])


# Upon clickin on the Submit button, a spark-submit command is sent to the cluster to process the user request
def submit_request(request):
    command = preprocess_query.preprocess(request)
    process = subprocess.Popen(command.split())
    flash(
        "Your request has been submitted. When it is finished you will receive an email. Thank you for your patience."
    )


@app.route("/", methods=["GET", "POST"])
def hello():
    form = ReusableForm(request.form)

    if request.method == "POST":
        if form.validate():
            submit_request(request)
        else:
            flash("Error: All Fields are Required")

    return render_template("index.html", form=form)


if __name__ == "__main__":
    app.run(host="0.0.0.0")
