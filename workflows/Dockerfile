# 1. Start with an official Python computer
FROM python:3.10-slim

# 2. Set the "working directory" inside the container
WORKDIR /app

# 3. Copy the shopping list inside
COPY requirements.txt .

# 4. Install all the software from the list
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy ALL your project files (data, scripts) inside
COPY . .

# 6. Set the default command (keeps the container running)
# This allows you to log into it with docker-compose
CMD ["tail", "-f", "/dev/null"]