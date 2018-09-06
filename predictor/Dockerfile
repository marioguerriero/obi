# Use Tensorflow Python 3 distribution
FROM eu.gcr.io/dhg-data-intelligence-ops/obi-predictor-env

# Set configuration env variables
ENV SERVICE_PORT 8080
ENV SERVICE_HOST 0.0.0.0

# Add necessary files
ADD * /
ADD predictors /predictors/

# Expose port endpoint
EXPOSE $SERVICE_PORT

# Run application
ENTRYPOINT ["python", "predictor_server.py"]
