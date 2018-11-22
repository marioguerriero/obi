FROM node:11.2.0

# Copy all files
ADD . /app/

# Expose server's port
ENV PORT 8080
EXPOSE $PORT

# Build
WORKDIR /app
RUN npm install
RUN npm run build

# Server resources
ENTRYPOINT ["npm", "run", "server"]
