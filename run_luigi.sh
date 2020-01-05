# Add ssh keys
eval `keychain --agents ssh --eval github_ssh`

# Go to desired path
cd /home/ubuntu/villoro_tasks/

# Git fetch and checkout
git fetch
git checkout master
git pull origin master
python src/slackbot.py Git fetch and pull

# Activate virtual environment
source /home/ubuntu/venv/vtasks/bin/activate
python src/slackbot.py Activate virtual env

# Install requirements
pip install -r requirements.txt
python src/slackbot.py Install requirements

# Run luigi
python src/master.py

# Deactivate virtual environment
deactivate
