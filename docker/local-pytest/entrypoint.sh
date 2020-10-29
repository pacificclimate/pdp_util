# Install PyCDS
pip3 install -r requirements.txt -r test_requirements.txt
pip3 install -e .

# Use a non-root user so that Postgres doesn't object
# Important: See README for reason user id 1000 is set here.
useradd -u 1000 test
chsh -s /bin/bash test
su test
