FROM python:3.11.9-slim

    EXPOSE 3001

    RUN apt-get update 
    RUN apt-get upgrade -y 
    RUN apt-get install -y --no-install-recommends nano curl
    
    COPY requirements.txt .
    RUN pip install --no-cache-dir -r requirements.txt
    
    WORKDIR /app
    COPY ./*.py /app/
    
    RUN apt-get autoremove -y && rm -rf /var/lib/apt/lists/*
    
    CMD ["python", "main.py"]