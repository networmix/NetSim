FROM python:3

# The WORKDIR instruction sets the working directory for any RUN, CMD, ENTRYPOINT,
# COPY and ADD instructions that follow it in the Dockerfile.
# If the WORKDIR doesn’t exist, it will be created even if it’s not used
# in any subsequent Dockerfile instruction.
WORKDIR /usr/src/NetSim

COPY netsim netsim
COPY examples examples
COPY LICENSE README.md requirements.txt setup.cfg setup.py .
RUN pip install -r requirements.txt
RUN pip install -e .

CMD ["/bin/bash"]
