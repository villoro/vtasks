# Add ssh keys
eval `keychain --agents ssh --eval github_ssh`

# Go to desired path
cd /home/ubuntu/villoro_tasks/

# Git fetch and checkout
git fetch
git checkout master
git pull origin master

# Activate virtual environment
source /home/ubuntu/venv/vtasks/bin/activate

# Install requirements
pip install -r requirements.txt

# Run luigi
python src/master.py

# Deactivate virtual environment
deactivate
