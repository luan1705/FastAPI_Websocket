FROM python:3.10-slim

# Cài đặt timezone Việt Nam
RUN apt-get update && apt-get install -y tzdata \
    && ln -snf /usr/share/zoneinfo/Asia/Ho_Chi_Minh /etc/localtime \
    && echo "Asia/Ho_Chi_Minh" > /etc/timezone \
    && apt-get clean

# Set working directory
WORKDIR /app

# Copy project files
COPY . /app

# Install dependencies
RUN pip install --upgrade pip \
    && pip install -r requirements.txt
    
# Expose port
EXPOSE 17000

# Run app
CMD ["uvicorn", "streaming:app", "--host", "0.0.0.0", "--port", "17000"]

