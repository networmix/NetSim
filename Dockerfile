FROM python:3

# Add Tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

# The WORKDIR instruction sets the working directory for any RUN, CMD, ENTRYPOINT,
# COPY and ADD instructions that follow it in the Dockerfile.
# If the WORKDIR doesn’t exist, it will be created even if it’s not used
# in any subsequent Dockerfile instruction.
WORKDIR /usr/src/NetSim

COPY netsim netsim
COPY examples examples
COPY LICENSE README.md requirements.txt setup.cfg setup.py /usr/src/NetSim/
RUN pip install jupyter
RUN pip install -r requirements.txt
RUN pip install -e .

ENTRYPOINT ["/tini", "-g", "--"]
CMD ["jupyter", "notebook", "--port=8888", "--no-browser", "--ip=0.0.0.0", "--allow-root", "examples/"]
