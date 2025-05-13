import smtplib
from email.mime.text import MIMEText

EMAIL_SENDER = "mango.butt3r@outlook.com"
EMAIL_PASSWORD = "mssp.qGn4bLg.vywj2lp7j9ml7oqz.VkmT2DZ"
EMAIL_RECEIVER = "mango.butt3r@gmail.com"

msg = MIMEText('Test email from CryptoBot via MailerSend')
msg['Subject'] = 'Test Email MailerSend'
msg['From'] = EMAIL_SENDER
msg['To'] = EMAIL_RECEIVER

try:
    with smtplib.SMTP('smtp.mailersend.net', 2525) as server:
        server.starttls()
        server.login("MS_a9UzWh@test-q3enl6kj9o842vwr.mlsender.net", EMAIL_PASSWORD)
        server.send_message(msg)
    print('Email sent')
except Exception as e:
    print(f'Failed to send email: {str(e)}')
