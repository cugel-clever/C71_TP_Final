# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copier le fichier de dépendances
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copier le reste du code de l'application (votre script pipeline.py, etc.)
COPY . .

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Define environment variable
ENV API_KEY=ede1c94c

# Run app.py when the container launches
CMD ["python", "pipeline.py"]
