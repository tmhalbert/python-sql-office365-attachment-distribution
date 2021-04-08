from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
todays_date = datetime.today()
import json

import logging
logging.basicConfig(format=f"""%(asctime)s  [%(levelname)s]\t%(message)s""",datefmt='%Y-%m-%d %H:%M:%S',level=logging.DEBUG)

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os.path


def get_data_from_sql(query) :
	engine = create_engine("""mssql+pyodbc://%s:%s@%s:1433/%s?driver=ODBC+Driver+17+for+SQL+Server""" % ('userbob','P@$$w0rd1','ip-0-0-0-0.ec2.internal','maindb'),echo=False)        
	df = pd.DataFrame()
	
	with engine.connect() as conn :
		df = pd.read_sql(query,conn)
		df.fillna('N/A',inplace=True)

	return df


def validate_email_recipient(recipient,allow_external) :
	if allow_external != 'true' :
		if '@contoso.com' not in recipient :        
			exception = f"The recipient\'s email address ({recipient}) is not permittied. All recipients must have an email in the 'contoso.com' domain."
			raise ValueError(exception)


def send_email_with_attachment(sender,recipients,subject,message,file) :
	msg = MIMEMultipart()
	username = 'noreply@contoso.com'
	dt = datetime.today()
	
	# setup the email header info
	try :
		msg['To'] = ', '.join(recipients)
		logging.debug(f'''Email recipient(s) set to {recipients}''')
		
		msg['From'] = f'{sender[0]} <{username}>'
		msg.add_header('reply-to',sender[1])
		logging.debug(f'''Email sender set to [{sender[0]} <{sender[1]}>]''')
		
		msg['Subject'] = subject    
		logging.debug(f'''Email Subject set to "{subject}"''')
	except Exception as error :
		raise ValueError(f'Failed to prepare email header. {error}')
	
	# append the message body and type
	try :
		msg.attach(MIMEText(message,'plain'))
		logging.debug(f'''Message body set to ::\n{message}''')
	except Exception as error :
		raise ValueError(f'Failed to prepare message body. {error}')

	# attach the prepared file
	try :
		filename = os.path.basename(file)
		logging.info(f'Preparing file attachment {filename}')

		file = open(file,'rb')
		
		part = MIMEBase('application','octet-stream')
		part.set_payload(file.read())
		encoders.encode_base64(part)
		part.add_header('Content-Disposition',f'file; filename={filename}')
		
		msg.attach(part)
	except Exception as error :
		raise ValueError(f'Failed to prepare file attachment. {error}')
	
	# send email
	try :
		server = smtplib.SMTP('smtp.office365.com',587)
		server.starttls()
		server.login(username,'P@$$w0rd2')
		text = msg.as_string()
		
		logging.debug('Connecting to the email server and sending email')
		server.sendmail(username,recipients,text)
		server.quit()
		logging.info(f'Email sent to {recipients} at {dt.strftime("%m/%d/%Y %H:%M:%S")}')
	except Exception as error :
		logging.error(f'Email failed to send. {error}')


def lambda_handler(event,context) :
	logging.info('Starting application')
	dt = todays_date.strftime("%B %d, %Y")

	event = event[0:1:]
	event = str(event[0]).replace("\'", "\"")
	event = json.loads(event)

	# set the sql query for pulling the data
	sql = event['sql']
	# set results filename
	filename = event['filename']
	# sender format ['Sender Name','reply-to email']
	sender = [event['sender'][0],event['sender'][1]]
	# recipients in a comma separated list
	recipients = event['recipients']
	# set subject and message body
	subject = event['subject'].format(dt=dt)
	message = event['message'].format(filename=filename)

	try :
		# validate there are valid recipients to recieve the file
		logging.debug('Validating recipient list')
		if len(recipients) > 0 :
			for recipient in recipients :
				try :
					validate_email_recipient(recipient,event['allow_external'])
				except Exception as error :
					logging.error(f'Invalid email recipient. {error}')
					recipients.remove(recipient)
		else :
			raise ValueError('Cannot continue without any email recipients.')

		if len(recipients) > 0 :
			# data query and prep
			logging.info('Extracting data from the data warehouse.')
			logging.debug(f'''Executing query at the data warehouse. Command sent ::\n{sql}''')
			try :
				df = get_data_from_sql(f'{sql}')
				# df = df.head()
			except Exception as error :
				raise ValueError(f'Unable to execute query. {error}')

			if df.shape[0] > 0 :
				logging.debug(f'{df.shape[0]} records returned from the data warehouse')
				logging.debug(f'Saving query results to file <{filename}>')

				try :
					df.to_csv(filename, header=True, index=False, line_terminator='\n')
				except Exception as error :
					raise ValueError(f'Unable to save query data to file. {error}')

				# send the email with attachment
				try :
					send_email_with_attachment(sender,recipients,subject,message,filename)
				except Exception as error :
					raise ValueError(f'Unable to send email. {error}')
			
			else :
				raise ValueError('Query returned no data.')
		else :
			raise ValueError(f'Cannot continue without any valid recipients.')
	except Exception as error :
		logging.critical(f'Application failed to run. {error}')


if __name__ == "__main__":
		execute = ["{\"sender\":[\"Contoso Data Team\",\"userbob@contoso.com\"],\"subject\":\"Your Daily Data Dump for {dt}\",\"message\":\"Hello,\\nAttached is your Daily Data Dump CSV file <{filename}>. Should you need any further information, please do not hesitate to contact me.\\n\\nThank you,\\nUser Bob\",\"recipients\":[\"email@contoso.com\",\"email@gmail.com\"],\"allow_external\":\"false\",\"filename\":\"data_dump.csv\",\"sql\":\"select * from table\"}"]
		lambda_handler(execute,None)
