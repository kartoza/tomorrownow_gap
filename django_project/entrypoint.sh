#!/bin/sh

# Exit script in case of error
set -e

echo $"\n\n\n"
echo "-----------------------------------------------------"
echo "STARTING DJANGO ENTRYPOINT $(date)"
echo "-----------------------------------------------------"

# Run NPM
cd /home/web/django_project/frontend
echo "npm install"
npm install --verbose
echo "generating theme types"
npx -y @chakra-ui/cli typegen ./src/theme.ts
echo "npm build"
npm run build

# copy flower daemon script
cd /home/web/django_project
rm -f /var/tmp/flower.pid
cp flower.sh /etc/init.d/flower
chmod +x /etc/init.d/flower
update-rc.d flower defaults
sleep 2
/etc/init.d/flower start

# Run initialization
cd /home/web/django_project
echo 'Running initialize.py...'
python -u initialize.py
python manage.py migrate

echo "-----------------------------------------------------"
echo "FINISHED DJANGO ENTRYPOINT --------------------------"
echo "-----------------------------------------------------"

# Run the CMD
exec "$@"