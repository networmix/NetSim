# Dockerfile for my PythonBuildEnv docker container

# Download base ubuntu image
FROM ubuntu:20.04

# Prevent running interactive config
ENV DEBIAN_FRONTEND noninteractive

# Update all package lists
RUN apt-get -y update

# Install required packages
RUN apt-get install -y build-essential wget git iproute2 vim

# Install Python dev packages
RUN apt-get install -y python3 python3-pip python3-venv

# Install nice-to-have packages to make debugging easier
RUN apt-get install -y net-tools tcpdump inetutils-ping

# Workaround to make TCPDump work in containers. See https://stackoverflow.com/questions/30663245
RUN mv /usr/sbin/tcpdump /usr/bin/tcpdump

# The VOLUME instruction creates a mount point with the specified name
# and marks it as holding externally mounted volumes from native host or other containers.
VOLUME /root/env

# The WORKDIR instruction sets the working directory for any RUN, CMD, ENTRYPOINT,
# COPY and ADD instructions that follow it in the Dockerfile.
# If the WORKDIR doesn’t exist, it will be created even if it’s not used
# in any subsequent Dockerfile instruction.
WORKDIR /root/env
CMD ["/bin/bash"]
