# Using the python:3.11-slim version of size 121 MB
FROM python:3.11-slim as build
# Adding requirements to the container for the packages
ADD requirements.txt /
# Adding config file to the container for credentials
ADD config.ini /
# Installing all requirements to the python environment
RUN pip install -r requirements.txt
# Adding the code  
ADD main.py /
# Keep the container active forever
CMD ["sleep", "infinity"]