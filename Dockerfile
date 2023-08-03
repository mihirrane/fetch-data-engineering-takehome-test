# Using the python:3.11-slim version of size 121 MB
FROM python:3.11-slim as build
# Adding requirements to the container for the packages
ADD requirements.txt /
# Installing all requirements to the python environment
RUN pip install -r requirements.txt
# Keep the container active forever
CMD ["sleep", "infinity"]