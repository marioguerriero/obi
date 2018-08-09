# Use Python 3
FROM python:3

# Set configuration env variables
ENV SERVICE_PORT 8080

# Add necessary files
ADD *.py /
ADD requirements.txt /

# Install dependencies
RUN pip install -m requirements.txt

# Expose port endpoint
EXPOSE $SERVICE_PORT

# Run application
CMD ['python', 'predictor_server.py']
