# Use an official Golang runtime as a parent image
FROM golang:1.22

# Install Python, pip, and net-tools
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    net-tools \
    openssl \
    && rm -rf /var/lib/apt/lists/*

# Configure git to use HTTPS instead of SSH
RUN git config --global url."https://github.com/".insteadOf "git@github.com:"

# Set the working directory
WORKDIR /app

# Generate SSL certificates for local development
RUN openssl req -x509 -newkey rsa:4096 -nodes \
    -out server.crt -keyout server.key \
    -days 365 -subj '/CN=localhost'

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies
RUN go mod download

# Create and activate Python virtual environment
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements.txt first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies in virtual environment
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy the source code
COPY . .

# Make startup script executable
RUN chmod +x startup.sh

# Set environment variables
ENV GO111MODULE=on
ENV PYTHONUNBUFFERED=1

# Expose ports for the services
EXPOSE 6070  
EXPOSE 5002  
EXPOSE 5003
EXPOSE 8080

# Command to run the startup script
ENTRYPOINT ["./startup.sh"]
