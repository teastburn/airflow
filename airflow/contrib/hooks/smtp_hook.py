from airflow.hooks.base_hook import BaseHook
import smtplib
import logging
from email.mime.multipart import MIMEMultipart as mime_MIMEMultipart
from email.mime.text import MIMEText as mime_MIMEText


class SMTPHook(BaseHook):
    """
    Interact with SMTP.
    """

    def __init__(self, smtp_conn_id='smtp_default'):
        self.smtp_conn_id = smtp_conn_id
        self.conn = None

    def get_conn(self):
        """
        Returns a SMTP connection object
        """
        if self.conn is None:
            params = self.get_connection(self.smtp_conn_id)
            if params.login:
                self.conn = smtplib.SMTP(params.host, params.port)
                # Negotiate SSL
                self.conn.starttls()
                # Auth with sendgrid
                self.conn.login(params.login, params.password)
            else:
                self.conn = smtplib.SMTP(params.host)

        return self.conn

    def send_email(self, email_from, email_to, message):
        """
        Send an email via SMPT

        :param email_from: str
        :param email_to: str
        :param message: str
        """
        logging.info("Attempting to send email from " + email_from + " to " + email_to)
        self.conn.sendmail(email_from, email_to, message)
        logging.info("Email sent")

    def send_html_email(self, email_from, email_to, subject, html_body):
        """
        Send an email with an html body

        :param email_from: str
        :param email_to: str
        :param subject: str
        :param html_body: str
        """
        msg = mime_MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = email_from
        msg['To'] = email_to

        html_part = mime_MIMEText(html_body, 'html', _charset='utf-8')
        msg.attach(html_part)

        self.send_email(email_from, email_to, msg.as_string())

    def close(self):
        if self.conn:
            self.conn.close()
