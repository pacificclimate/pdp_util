# Install pdp_util
pipenv install

# Use a non-root user so that Postgres doesn't object
# Important: See README for reason user id 1000 is set here.
useradd -u 1000 test
chsh -s /bin/bash test
su test
