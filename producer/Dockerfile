# Get the base image 
FROM python:3.11-alpine

# dont write pyc files
ENV PYTHONDONTWRITEBYTECODE 1

# dont buffer to stdout/stderr# 
ENV PYTHONUNBUFFERED 1
# Add the necessary packages 
USER root 
RUN apt update 
RUN apt add --no-cache curl 
RUN apk add --no-cache bash 

# Make a directory for the app and copy the files  
WORKDIR /producer 
COPY ./app ./app


# Run the application 
CMD [ "python" ,"app/producer.py" ]




