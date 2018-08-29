# Use Tensorflow Python 3 distribution
FROM python:3.5

# Set configuration env variables
ENV SERVICE_PORT 8080
ENV SERVICE_HOST 0.0.0.0

# Install gcsfuse
RUN apt-get update && apt-get install --yes --no-install-recommends \
    ca-certificates \
    curl \
  && echo "deb http://packages.cloud.google.com/apt $GCSFUSE_REPO main" \
    | tee /etc/apt/sources.list.d/gcsfuse.list \
  && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
  && apt-get update \
  && apt-get install --yes gcsfuse \
  && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Add necessary files
ADD * /
ADD models/ /models/
ADD predictors /profiles/

# Install dependencies
RUN pip install -r /requirements.txt

# Expose port endpoint
EXPOSE $SERVICE_PORT

# Run application
ENTRYPOINT ["python", "predictor_server.py"]
