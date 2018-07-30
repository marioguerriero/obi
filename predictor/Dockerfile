# Use Python 3
FROM python:3

# Add necessary files
ADD *.py /
ADD requirements.txt /

# Install dependencies
RUN pip install -m requirements.txt

# Run application
CMD ['FLASK_APP=predictor_server.py', 'flask run']
