FROM python:3.8

# Install the required libraries
RUN pip install boto3 psycopg2 pandas

# Copy the application code and dependencies to the container
COPY . /app
WORKDIR /app

# Set the entrypoint of the container to the main script
ENTRYPOINT ["python", "main.py"]
