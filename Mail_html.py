import smtplib
import logging
import os

from email.mime.text import MIMEText

# logging.basicConfig(filename = os.path.join(os.getcwd(), 'monitor-hdfs.log'), level = logging.INFO,format = '%(asctime)s - %(levelname)s: %(message)s')


mailto = ""
mail_host = "smtp.qiye.163.com"
mail_user = "data_warning@xxx.com"
mail_pass = "xxxx"
mail_postfix = "xxx.com"


def send_mail(sub, content, to):
    me = "data_warning" + "<" + mail_user + "@" + mail_postfix + ">"
    # msg = MIMEText(content,_subtype='plain',_charset='gb2312')
    msg = MIMEText(content, _subtype='html', _charset='utf-8')
    msg['Subject'] = sub
    msg['From'] = me
    msg['To'] = ", ".join(to)

    try:
        server = smtplib.SMTP()
        server.connect(mail_host)
        server.login(mail_user, mail_pass)
        server.sendmail(me, to, msg.as_string())
        server.close()
        logging.info("MAIL SENT")
        return True

    except Exception, e:
        logging.info("FAIL")
        logging.info(str(e))
        return False

