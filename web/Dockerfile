FROM eu.gcr.io/dhg-data-intelligence-ops/obi-web-env

# Copy all files
ADD . /app/

# Open port
ENV PORT 8084
EXPOSE $PORT

# Build
WORKDIR /app
RUN npm run build

ENTRYPOINT ["npm", "run", "prod"]
