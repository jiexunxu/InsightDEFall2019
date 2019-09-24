# Send an email notification to user, provide a download link and save the user's query as a log in a file on S3

import smtplib
from email.message import EmailMessage

def email_and_log(user_email, user_selection, user_param):
    def email():
        msg=EmailMessage()
        msg.set_content("Your download link is ready at ")
        msg['Subject']="Your data is ready"
        msg['From']="service"
        msg['To']=user_email
        s=smtplib.SMTP('localhost')
        s.send_message(msg)
        s.quit()
    
    def log():
        with open("boise_log.txt", "a+") as f:
            f.write(user_email+',')
            for i in range(len(user_selection)-1):
                f.write(str(user_param[i])+',')
            for i in range(len(user_param)-1):
                f.write(str(user_param[i])+',')
            f.write(str(user_param[len(user_param)-1])+'\n')
    
    email()
    log()
        
