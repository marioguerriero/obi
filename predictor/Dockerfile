# Use Tensorflow Python 3 distribution
FROM python:3.5

# Set configuration env variables
ENV SERVICE_PORT 8080
ENV SERVICE_HOST 0.0.0.0

# Add necessary files
ADD * /
ADD models/ /models/
ADD profiles/ /profiles/

# Install dependencies
RUN pip install -r /requirements.txt

# Expose port endpoint
EXPOSE $SERVICE_PORT

# Run application
ENTRYPOINT ["python", "predictor_server.py"]
