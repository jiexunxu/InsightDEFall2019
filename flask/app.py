import sys
sys.path.insert(0, './flask')
sys.path.insert(0, './spark')
sys.path.insert(0, './python')
from flask import Flask, render_template, flash, request
from wtforms import Form, TextField, TextAreaField, validators, StringField, SubmitField
import preprocess_query
import subprocess


DEBUG = True
app = Flask(__name__)
app.config.from_object(__name__)
app.config['SECRET_KEY'] = 'SjdnUends821Jsdlkvxh391ksdODnejdDw'

class ReusableForm(Form):
    email = TextField('', validators=[validators.required()])
    obj_count = TextField('', validators=[validators.required()])
    box_source = TextField('', validators=[validators.required()])
    labels = TextField('', validators=[validators.required()])
    image_size = TextField('', validators=[validators.required()])
    scale = TextField('', validators=[validators.required()])
    crop = TextField('', validators=[validators.required()])
    blur = TextField('', validators=[validators.required()])

def submit_request(request):
    command=preprocess_query.preprocess(request)
    process=subprocess.Popen(command.split())
    flash("Your request has been submitted. When it is finished you will receive an email. Thank you for your patience.")

@app.route("/", methods=['GET', 'POST'])
def hello():
    form = ReusableForm(request.form)
    
    if request.method == 'POST':
        if form.validate():
            submit_request(request)
        else:
            flash('Error: All Fields are Required')

    return render_template('index.html', form=form)

if __name__ == "__main__":
    app.run(host='0.0.0.0')
