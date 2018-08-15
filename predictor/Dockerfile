# Use Tensorflow Python 3 distribution
FROM tensorflow/tensorflow:latest-py3

# Set configuration env variables
ENV SERVICE_PORT 8080
ENV SERVICE_HOST 0.0.0.0

# Add necessary files
ADD *.py /
ADD requirements.txt /

# Install dependencies
RUN pip install -r /requirements.txt

# Expose port endpoint
EXPOSE $SERVICE_PORT

# Run application
CMD ['python', 'predictor_server.py']
