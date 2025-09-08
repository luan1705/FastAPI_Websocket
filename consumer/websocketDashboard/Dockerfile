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
RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt

# Expose port
EXPOSE 8000

# Run app
CMD ["uvicorn", "ws_dashboard:app", "--host", "0.0.0.0", "--port", "8000"]

ENV PYTHONPATH=/app
