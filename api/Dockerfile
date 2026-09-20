# Use official, lightweight Python image
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# We don't need to copy the code here because we are using a 'volume' 
# in docker-compose to mount our live code into the container.

# Command to run the application using uvicorn (the standard ASGI server)
# --reload allows for hot-reloading when we save a Python file
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]