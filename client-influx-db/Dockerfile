FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the Python script and requirements file into the container
COPY opc-ua-client.py requirements.txt ./

# Install necessary dependencies
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "opc-ua-client.py"]